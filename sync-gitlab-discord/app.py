import os
import httpx
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from fastapi import FastAPI, Header, HTTPException, Request


@dataclass(frozen=True)
class RelayConfig:
    discord_webhook_url: str
    gitlab_secret: str
    allowed_push_refs: set[str]
    only_mr_notes: bool
    ignore_system_notes: bool

    @staticmethod
    def from_env() -> "RelayConfig":
        discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        gitlab_secret = os.getenv("GITLAB_SECRET", "").strip()
        allowed_push_refs = {s.strip() for s in os.getenv("ALLOWED_PUSH_REFS", "").split(",") if s.strip()}
        only_mr_notes = os.getenv("ONLY_MR_NOTES", "1").strip().lower() in {"1", "true", "yes", "on"}
        ignore_system_notes = os.getenv("IGNORE_SYSTEM_NOTES", "1").strip().lower() in {"1", "true", "yes", "on"}

        return RelayConfig(
            discord_webhook_url=discord_webhook_url,
            gitlab_secret=gitlab_secret,
            allowed_push_refs=allowed_push_refs,
            only_mr_notes=only_mr_notes,
            ignore_system_notes=ignore_system_notes,
        )


class DiscordClient:
    def __init__(self, webhook_url: str):
        self._webhook_url = webhook_url.strip()

    async def post(self, payload: dict[str, Any]) -> None:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.post(self._webhook_url, json=payload)
            if r.status_code >= 300:
                raise HTTPException(status_code=502, detail=f"Discord error {r.status_code}: {r.text}")


@dataclass(frozen=True)
class GitLabEventContext:
    event: str
    payload: dict[str, Any]
    project: str
    project_url: str
    user_name: str
    user_avatar: str | None


class IGitLabEventHandler(Protocol):
    def can_handle(self, ctx: GitLabEventContext) -> bool: ...
    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None: ...


class Formatter:
    @staticmethod
    def pick(d: dict, *path, default=None):
        cur = d
        for k in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
        return cur if cur is not None else default

    @staticmethod
    def iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def short(text: str, limit: int = 256) -> str:
        text = (text or "").strip()
        return text if len(text) <= limit else text[: limit - 3] + "..."

    @staticmethod
    def short_desc(text: str, limit: int = 1800) -> str:
        text = (text or "").strip()
        return text if len(text) <= limit else text[: limit - 3] + "..."

    @staticmethod
    def color(kind: str) -> int:
        k = (kind or "").lower()
        if k == "mr":
            return 0x3498DB
        if k == "note":
            return 0x9B59B6
        if k == "push":
            return 0x1ABC9C
        if k == "wiki":
            return 0xF39C12
        return 0x95A5A6

    @staticmethod
    def mr_title(action: str | None) -> str:
        a = (action or "").lower()
        return {
            "open": "Merge Request открыт",
            "reopen": "Merge Request переоткрыт",
            "update": "Merge Request обновлён",
            "approved": "Merge Request одобрен",
            "unapproved": "Merge Request снят с одобрения",
            "merge": "Merge Request смержен",
            "close": "Merge Request закрыт",
        }.get(a, f"Merge Request: {action or 'event'}")

    @staticmethod
    def note_title(noteable_type: str | None) -> str:
        t = (noteable_type or "").lower()
        return {
            "mergerequest": "Комментарий к MR",
            "issue": "Комментарий к Issue",
            "commit": "Комментарий к Commit",
            "snippet": "Комментарий к Snippet",
        }.get(t, f"Комментарий: {noteable_type or 'note'}")


class MergeRequestHandler:
    def __init__(self):
        self._fmt = Formatter()

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Merge Request Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        attrs = ctx.payload.get("object_attributes") or {}
        action = attrs.get("action")
        title = attrs.get("title") or "MR"
        url = attrs.get("url") or ""
        src = attrs.get("source_branch") or "?"
        tgt = attrs.get("target_branch") or "?"
        state = attrs.get("state") or ""
        iid = attrs.get("iid") or attrs.get("id") or ""

        embed = {
            "title": f"!{iid} • {self._fmt.mr_title(action)}",
            "url": url or ctx.project_url or None,
            "description": self._fmt.short_desc(title),
            "color": self._fmt.color("mr"),
            "timestamp": self._fmt.iso_now(),
            "author": {"name": ctx.user_name, **({"icon_url": ctx.user_avatar} if ctx.user_avatar else {})},
            "fields": [
                {"name": "Проект", "value": f"[{ctx.project}]({ctx.project_url})" if ctx.project_url else ctx.project, "inline": True},
                {"name": "Ветки", "value": f"`{src}` → `{tgt}`", "inline": True},
                {"name": "Статус", "value": f"`{state or 'unknown'}`", "inline": True},
            ],
        }

        return {"content": None, "embeds": [embed]}


class PushHandler:
    def __init__(self, cfg: RelayConfig):
        self._cfg = cfg
        self._fmt = Formatter()

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Push Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        ref = ctx.payload.get("ref") or "?"
        if self._cfg.allowed_push_refs and ref not in self._cfg.allowed_push_refs:
            return None

        commits = ctx.payload.get("commits") or []
        count = len(commits)
        before = ctx.payload.get("before") or ""
        after = ctx.payload.get("after") or ""
        compare_url = ctx.payload.get("compare") or ""

        branch = ref.replace("refs/heads/", "")
        lines = []
        for c in commits[:5]:
            cid = (c.get("id") or "")[:8]
            msg = (c.get("message") or "").split("\n")[0].strip()
            lines.append(f"`{cid}` {msg}")
        if count > 5:
            lines.append(f"_ещё {count - 5}…_")

        embed = {
            "title": f"Push в `{ctx.project}`",
            "url": compare_url or ctx.project_url or None,
            "description": self._fmt.short_desc("\n".join(lines) if lines else "Без списка коммитов"),
            "color": self._fmt.color("push"),
            "timestamp": self._fmt.iso_now(),
            "author": {"name": ctx.user_name, **({"icon_url": ctx.user_avatar} if ctx.user_avatar else {})},
            "fields": [
                {"name": "Ветка", "value": f"`{branch}`", "inline": True},
                {"name": "Коммиты", "value": f"`{count}`", "inline": True},
                {"name": "Diff", "value": f"`{before[:8]}` → `{after[:8]}`", "inline": True},
            ],
        }

        return {"content": None, "embeds": [embed]}


class NoteHandler:
    def __init__(self, cfg: RelayConfig):
        self._cfg = cfg
        self._fmt = Formatter()

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Note Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        attrs = ctx.payload.get("object_attributes") or {}

        if self._cfg.ignore_system_notes and attrs.get("system"):
            return None

        noteable_type = (attrs.get("noteable_type") or "").lower()
        if self._cfg.only_mr_notes and noteable_type != "mergerequest":
            return None

        note = attrs.get("note") or ""
        url = attrs.get("url") or ""

        mr = ctx.payload.get("merge_request") or {}
        issue = ctx.payload.get("issue") or {}
        commit = ctx.payload.get("commit") or {}

        target_title = (
            mr.get("title")
            or issue.get("title")
            or commit.get("title")
            or commit.get("message")
            or "Объект"
        )

        target_url = mr.get("url") or issue.get("url") or url or ctx.project_url or None

        fields = [{"name": "Проект", "value": f"[{ctx.project}]({ctx.project_url})" if ctx.project_url else ctx.project, "inline": True}]

        if mr.get("source_branch") or mr.get("target_branch"):
            fields.append(
                {"name": "Ветки", "value": f"`{mr.get('source_branch','?')}` → `{mr.get('target_branch','?')}`", "inline": True}
            )

        if commit.get("id"):
            fields.append({"name": "Commit", "value": f"`{str(commit.get('id'))[:8]}`", "inline": True})

        embed = {
            "title": self._fmt.note_title(noteable_type),
            "url": target_url,
            "description": self._fmt.short_desc(note),
            "color": self._fmt.color("note"),
            "timestamp": self._fmt.iso_now(),
            "author": {"name": ctx.user_name, **({"icon_url": ctx.user_avatar} if ctx.user_avatar else {})},
            "fields": [
                {"name": "Тема", "value": self._fmt.short(str(target_title), 512), "inline": False},
                *fields,
            ],
        }

        return {"content": None, "embeds": [embed]}


class WikiHandler:
    def __init__(self):
        self._fmt = Formatter()

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Wiki Page Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        attrs = ctx.payload.get("object_attributes") or {}
        action = attrs.get("action") or "updated"
        title = attrs.get("title") or attrs.get("slug") or "Wiki"
        url = attrs.get("url") or ""
        message = attrs.get("message") or ""

        embed = {
            "title": f"Wiki {action}: {self._fmt.short(title, 240)}",
            "url": url or ctx.project_url or None,
            "description": self._fmt.short_desc(message) if message else None,
            "color": self._fmt.color("wiki"),
            "timestamp": self._fmt.iso_now(),
            "author": {"name": ctx.user_name, **({"icon_url": ctx.user_avatar} if ctx.user_avatar else {})},
            "fields": [
                {"name": "Проект", "value": f"[{ctx.project}]({ctx.project_url})" if ctx.project_url else ctx.project, "inline": True},
            ],
        }

        return {"content": None, "embeds": [embed]}


class EventRouter:
    def __init__(self, handlers: list[IGitLabEventHandler]):
        self._handlers = handlers

    async def route(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        for h in self._handlers:
            if h.can_handle(ctx):
                return await h.handle(ctx)
        return None


cfg = RelayConfig.from_env()
app = FastAPI()

if not cfg.discord_webhook_url:
    raise RuntimeError("DISCORD_WEBHOOK_URL is not set")

discord = DiscordClient(cfg.discord_webhook_url)
router = EventRouter(
    handlers=[
        MergeRequestHandler(),
        PushHandler(cfg),
        NoteHandler(cfg),
        WikiHandler(),
    ]
)


@app.post("/gitlab")
async def gitlab_hook(
    request: Request,
    x_gitlab_token: str | None = Header(default=None),
    x_gitlab_event: str | None = Header(default=None),
):
    if cfg.gitlab_secret and (x_gitlab_token or "") != cfg.gitlab_secret:
        raise HTTPException(status_code=401, detail="Bad token")

    payload: dict[str, Any] = await request.json()

    event = (x_gitlab_event or "").strip()
    project = Formatter.pick(payload, "project", "path_with_namespace") or Formatter.pick(payload, "project", "name") or "unknown"
    project_url = Formatter.pick(payload, "project", "web_url") or ""
    user_name = payload.get("user_name") or Formatter.pick(payload, "user", "name") or "someone"
    user_avatar = payload.get("user_avatar") or Formatter.pick(payload, "user", "avatar_url") or None

    ctx = GitLabEventContext(
        event=event,
        payload=payload,
        project=project,
        project_url=project_url,
        user_name=user_name,
        user_avatar=user_avatar,
    )

    out = await router.route(ctx)
    if not out:
        return {"ok": True, "ignored": True}

    await discord.post(out)
    return {"ok": True}
