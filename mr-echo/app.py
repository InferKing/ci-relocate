import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException
import uvicorn


UTC = timezone.utc


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_gitlab_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Settings:
    discord_webhook_url: str
    gitlab_url: str
    gitlab_token: str
    gitlab_projects: List[str]
    stale_after_hours: int
    remind_every_hours: int
    notify_draft_mrs: bool
    listen_host: str
    listen_port: int
    check_interval_minutes: int
    state_file: Path
    trigger_token: str

    @staticmethod
    def from_env() -> "Settings":
        gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com").strip().rstrip("/")
        state_file = Path(os.getenv("STATE_FILE", "./bot-state.json")).expanduser()

        settings = Settings(
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
            gitlab_url=gitlab_url,
            gitlab_token=os.getenv("GITLAB_TOKEN", "").strip(),
            gitlab_projects=parse_csv(os.getenv("GITLAB_PROJECTS")),
            stale_after_hours=int(os.getenv("STALE_AFTER_HOURS", "48")),
            remind_every_hours=int(os.getenv("REMIND_EVERY_HOURS", "24")),
            notify_draft_mrs=parse_bool(os.getenv("NOTIFY_DRAFT_MRS"), default=False),
            listen_host=os.getenv("HOST", "0.0.0.0").strip(),
            listen_port=int(os.getenv("PORT", "8000")),
            check_interval_minutes=int(os.getenv("CHECK_INTERVAL_MINUTES", "30")),
            state_file=state_file,
            trigger_token=os.getenv("TRIGGER_TOKEN", "").strip(),
        )

        if not settings.discord_webhook_url:
            raise RuntimeError("DISCORD_WEBHOOK_URL is not set")
        if not settings.gitlab_token:
            raise RuntimeError("GITLAB_TOKEN is not set")
        if not settings.gitlab_projects:
            raise RuntimeError("GITLAB_PROJECTS is not set")

        return settings


class JsonStateStore:
    def __init__(self, path: Path):
        self._path = path
        self._lock = asyncio.Lock()
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self._path.exists():
            return {"notifications": {}}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"notifications": {}}
        if not isinstance(raw, dict):
            return {"notifications": {}}
        raw.setdefault("notifications", {})
        return raw

    async def get_last_sent_at(self, key: str) -> Optional[datetime]:
        async with self._lock:
            value = self._data.get("notifications", {}).get(key)
        return parse_gitlab_datetime(value)

    async def set_last_sent_at(self, key: str, value: datetime) -> None:
        async with self._lock:
            self._data.setdefault("notifications", {})[key] = value.isoformat()
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(self._data, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )


class DiscordClient:
    def __init__(self, webhook_url: str):
        self._webhook_url = webhook_url

    async def post(self, payload: Dict[str, Any]) -> None:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(self._webhook_url, json=payload)
        if response.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Discord error {response.status_code}: {response.text}")


class GitLabClient:
    def __init__(self, base_url: str, token: str):
        self._base_url = base_url
        self._headers = {"PRIVATE-TOKEN": token}

    async def list_open_merge_requests(self, project: str) -> List[Dict[str, Any]]:
        encoded_project = quote(project, safe="")
        url = f"{self._base_url}/api/v4/projects/{encoded_project}/merge_requests"
        params = {
            "state": "opened",
            "scope": "all",
            "per_page": 100,
            "order_by": "updated_at",
            "sort": "asc",
        }

        async with httpx.AsyncClient(timeout=20, headers=self._headers) as client:
            response = await client.get(url, params=params)

        if response.status_code >= 300:
            raise HTTPException(
                status_code=502,
                detail=f"GitLab error {response.status_code} for project {project}: {response.text}",
            )
        data = response.json()
        if not isinstance(data, list):
            raise HTTPException(status_code=502, detail=f"Unexpected GitLab response for project {project}")
        return data


class StaleMergeRequestService:
    def __init__(self, settings: Settings, gitlab: GitLabClient, discord: DiscordClient, state: JsonStateStore):
        self._settings = settings
        self._gitlab = gitlab
        self._discord = discord
        self._state = state

    async def run_check(self) -> Dict[str, Any]:
        stale_mrs: List[Dict[str, Any]] = []
        now = utc_now()
        stale_before = now - timedelta(hours=self._settings.stale_after_hours)

        for project in self._settings.gitlab_projects:
            project_mrs = await self._gitlab.list_open_merge_requests(project)
            for mr in project_mrs:
                if not self._should_notify_for_mr(mr, stale_before):
                    continue
                if not await self._notification_due(mr, now):
                    continue
                stale_mrs.append(mr)

        for mr in stale_mrs:
            payload = self._build_discord_payload(mr, now)
            await self._discord.post(payload)
            await self._state.set_last_sent_at(self._notification_key(mr), now)

        return {
            "checked_projects": len(self._settings.gitlab_projects),
            "sent_notifications": len(stale_mrs),
            "stale_after_hours": self._settings.stale_after_hours,
            "checked_at": now.isoformat(),
        }

    def _should_notify_for_mr(self, mr: Dict[str, Any], stale_before: datetime) -> bool:
        if mr.get("state") != "opened":
            return False
        if not self._settings.notify_draft_mrs and (mr.get("draft") or str(mr.get("title") or "").startswith("Draft:")):
            return False

        updated_at = parse_gitlab_datetime(mr.get("updated_at"))
        if updated_at is None:
            return False
        return updated_at <= stale_before

    async def _notification_due(self, mr: Dict[str, Any], now: datetime) -> bool:
        last_sent_at = await self._state.get_last_sent_at(self._notification_key(mr))
        if last_sent_at is None:
            return True
        return now - last_sent_at >= timedelta(hours=self._settings.remind_every_hours)

    def _notification_key(self, mr: Dict[str, Any]) -> str:
        project_id = str(mr.get("project_id") or "unknown")
        iid = str(mr.get("iid") or mr.get("id") or "unknown")
        return f"{project_id}:{iid}"

    def _build_discord_payload(self, mr: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        updated_at = parse_gitlab_datetime(mr.get("updated_at"))
        created_at = parse_gitlab_datetime(mr.get("created_at"))
        author = mr.get("author") or {}
        assignees = mr.get("assignees") or []
        reviewers = mr.get("reviewers") or []
        labels = mr.get("labels") or []
        web_url = mr.get("web_url") or self._settings.gitlab_url

        assignee_names = ", ".join(user.get("name", "unknown") for user in assignees) or "n/a"
        reviewer_names = ", ".join(user.get("name", "unknown") for user in reviewers) or "n/a"
        label_text = ", ".join(str(label) for label in labels[:6]) or "n/a"

        age_text = self._format_age(created_at, now)
        stale_text = self._format_age(updated_at, now)
        title = str(mr.get("title") or "Untitled MR").strip()
        project_name = str(mr.get("references", {}).get("full") or mr.get("reference") or f"!{mr.get('iid', '?')}")

        embed = {
            "title": f"Stale MR: {project_name}",
            "url": web_url,
            "description": title[:1800],
            "color": 0xE67E22,
            "timestamp": now.isoformat(),
            "author": {
                "name": author.get("name", "unknown"),
                **({"icon_url": author["avatar_url"]} if author.get("avatar_url") else {}),
            },
            "fields": [
                {"name": "Updated", "value": stale_text, "inline": True},
                {"name": "Age", "value": age_text, "inline": True},
                {"name": "Target", "value": f"`{mr.get('source_branch', '?')}` -> `{mr.get('target_branch', '?')}`", "inline": True},
                {"name": "Assignees", "value": assignee_names[:1024], "inline": False},
                {"name": "Reviewers", "value": reviewer_names[:1024], "inline": False},
                {"name": "Labels", "value": label_text[:1024], "inline": False},
            ],
        }

        return {"content": None, "embeds": [embed]}

    def _format_age(self, dt: Optional[datetime], now: datetime) -> str:
        if dt is None:
            return "unknown"
        delta = now - dt
        total_hours = int(delta.total_seconds() // 3600)
        days, hours = divmod(total_hours, 24)
        if days:
            return f"{days}d {hours}h ago"
        return f"{hours}h ago"


settings = Settings.from_env()
state_store = JsonStateStore(settings.state_file)
gitlab_client = GitLabClient(settings.gitlab_url, settings.gitlab_token)
discord_client = DiscordClient(settings.discord_webhook_url)
service = StaleMergeRequestService(settings, gitlab_client, discord_client, state_store)

app = FastAPI(title="mr-echo")


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/check")
async def check_stale_mrs(x_trigger_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    if settings.trigger_token and (x_trigger_token or "") != settings.trigger_token:
        raise HTTPException(status_code=401, detail="Bad trigger token")
    return await service.run_check()


@app.on_event("startup")
async def startup_event() -> None:
    async def scheduler_loop() -> None:
        while True:
            try:
                await service.run_check()
            except Exception as exc:
                print(f"[mr-echo] scheduled check failed: {exc}")
            await asyncio.sleep(settings.check_interval_minutes * 60)

    asyncio.create_task(scheduler_loop())


if __name__ == "__main__":
    uvicorn.run(app, host=settings.listen_host, port=settings.listen_port)
