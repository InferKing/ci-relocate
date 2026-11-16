import asyncio
import json
import logging
import sqlite3
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from config import RelayConfig
from discord_client import DiscordClient, DiscordError, MessageNotFound
from handlers import EventRouter, Formatter, GitLabEventContext, MergeRequestHandler, NoteHandler, PushHandler, WikiHandler
from payloads import EVENT_MODELS, validate_payload
from state import MessageStore

logger = logging.getLogger("gitlab_discord")


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        data = {"level": record.levelname, "event": record.getMessage()}
        for name in ("event_id", "event_type", "project_id", "result", "elapsed_ms", "error_kind", "discord_status"):
            if hasattr(record, name):
                data[name] = getattr(record, name)
        return json.dumps(data, ensure_ascii=False)


def configure_logging(level: str):
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
    logger.setLevel(level)
    # HTTPX logs request URLs at INFO; a webhook URL contains its access token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def event_time(payload: dict) -> float | None:
    attrs = payload["object_attributes"]
    value = attrs.get("actioned_at") or attrs.get("updated_at")
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00").replace(" UTC", "+00:00"))
        return stamp.replace(tzinfo=timezone.utc).timestamp() if stamp.tzinfo is None else stamp.timestamp()
    except ValueError:
        return None


class MergeRequestCards:
    def __init__(self, discord: DiscordClient, store: MessageStore):
        self._discord, self._store = discord, store
        # Bounded locks serialize concurrent deliveries of the same MR in this process.
        self._locks = tuple(asyncio.Lock() for _ in range(64))

    async def deliver(self, ctx: GitLabEventContext, payload: dict) -> str:
        iid = ctx.payload["object_attributes"]["iid"]
        key = (self._discord.destination_key, ctx.project_key, iid)
        async with self._locks[hash(key) % len(self._locks)]:
            saved = self._store.get(*key)
            stamp = event_time(ctx.payload)
            if saved and stamp is not None and saved.event_time is not None and stamp < saved.event_time:
                return "mr_ignored_stale"
            if saved:
                try:
                    await self._discord.edit(saved.message_id, payload)
                    message_id, result = saved.message_id, "mr_updated"
                except MessageNotFound:
                    message_id, result = await self._discord.create(payload), "mr_recreated"
            else:
                message_id, result = await self._discord.create(payload), "mr_created"
            self._store.save(*key, message_id, stamp if stamp is not None else saved.event_time if saved else None)
            return result


def create_app(config: RelayConfig | None = None, *, discord_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        cfg = config or RelayConfig.from_env()
        configure_logging(cfg.log_level)
        store = MessageStore(cfg.state_db_path)
        try:
            async with httpx.AsyncClient(timeout=12, transport=discord_transport, follow_redirects=False) as client:
                discord = DiscordClient(cfg.discord_webhook_url, client)
                application.state.store = store
                application.state.discord = discord
                application.state.cards = MergeRequestCards(discord, store)
                application.state.router = EventRouter([
                    MergeRequestHandler(), PushHandler(cfg), NoteHandler(cfg), WikiHandler()
                ])
                application.state.counters = Counter()
                logger.info("relay_started")
                yield
        finally:
            store.close()
            logger.info("relay_stopped")

    application = FastAPI(title="GitLab Discord Relay", lifespan=lifespan)

    @application.get("/health")
    async def health(request: Request):
        try:
            request.app.state.store.ping()
        except (sqlite3.Error, AttributeError):
            return JSONResponse(status_code=503, content={"ok": False, "state_store": "unavailable"})
        return {"ok": True, "state_store": "ok", "counters": dict(request.app.state.counters)}

    @application.post("/gitlab")
    async def gitlab_hook(
        request: Request,
        x_gitlab_event: str | None = Header(default=None),
    ):
        started = time.monotonic()
        event = (x_gitlab_event or "").strip()
        event_id = next((request.headers.get(name) for name in
                         ("webhook-id", "Idempotency-Key", "X-Gitlab-Event-UUID") if request.headers.get(name)), str(uuid4()))
        log = {"event_id": event_id[:128], "event_type": event[:80], "result": "invalid"}
        counters = request.app.state.counters
        counters["received"] += 1
        try:
            try:
                raw = await request.json()
            except (ValueError, UnicodeDecodeError):
                raise HTTPException(400, "Invalid JSON") from None
            if not isinstance(raw, dict):
                raise HTTPException(422, "Webhook body must be a JSON object")
            if event not in EVENT_MODELS:
                counters["ignored"] += 1
                log["result"] = "unsupported_event"
                return {"ok": True, "ignored": True}
            try:
                payload = validate_payload(event, raw)
            except ValidationError as error:
                details = error.errors(include_input=False, include_context=False, include_url=False)
                raise HTTPException(422, details) from None
            log["project_id"] = (payload.get("project") or {}).get("id")
            ctx = GitLabEventContext(
                event=event, payload=payload,
                project=Formatter.pick(payload, "project", "path_with_namespace") or Formatter.pick(payload, "project", "name") or "unknown",
                project_url=Formatter.pick(payload, "project", "web_url") or "",
                user_name=payload.get("user_name") or Formatter.pick(payload, "user", "name") or Formatter.pick(payload, "user", "username") or "someone",
                user_avatar=payload.get("user_avatar") or Formatter.pick(payload, "user", "avatar_url"),
            )
            out = await request.app.state.router.route(ctx)
            if out is None:
                counters["ignored"] += 1
                log["result"] = "filtered"
                return {"ok": True, "ignored": True}
            if event == "Merge Request Hook":
                result = await request.app.state.cards.deliver(ctx, out)
            else:
                await request.app.state.discord.create(out)
                result = "sent"
            log["result"] = result
            if result == "mr_ignored_stale":
                counters["ignored"] += 1
                return {"ok": True, "ignored": True, "reason": "stale_mr_event"}
            counters["delivered"] += 1
            counters[result] += 1
            return {"ok": True, "action": result}
        except DiscordError as error:
            counters["failed"] += 1
            log.update(result="discord_error", error_kind=error.kind, discord_status=error.status_code)
            raise HTTPException(502, "Discord delivery failed; see relay logs by event_id") from None
        except (sqlite3.Error, OSError):
            counters["failed"] += 1
            log["result"] = "state_error"
            raise HTTPException(503, "State store is unavailable") from None
        except HTTPException:
            counters["invalid"] += 1
            raise
        finally:
            log["elapsed_ms"] = round((time.monotonic() - started) * 1000, 1)
            logger.log(logging.WARNING if log["result"] in {"invalid", "discord_error", "state_error"} else logging.INFO,
                       "webhook_processed", extra=log)

    return application


app = create_app()
