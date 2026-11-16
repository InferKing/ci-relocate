from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlsplit

from config import RelayConfig


@dataclass(frozen=True)
class GitLabEventContext:
    event: str
    payload: dict[str, Any]
    project: str
    project_url: str
    user_name: str
    user_avatar: str | None

    @property
    def project_key(self) -> str:
        project = self.payload.get("project") or {}
        if project.get("id"):
            try:
                origin = urlsplit(self.project_url).netloc.lower()
            except ValueError:
                origin = ""
            return f"{origin}:id:{project['id']}"
        return self.project_url.rstrip("/") or self.project


class IGitLabEventHandler(Protocol):
    def can_handle(self, ctx: GitLabEventContext) -> bool: ...
    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None: ...


class Formatter:
    @staticmethod
    def pick(data: dict, *path, default=None):
        current = data
        for key in path:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
        return current if current is not None else default

    @staticmethod
    def short(value: Any, limit: int = 256) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[:limit - 3] + "..." if limit >= 3 else text[:limit]

    @staticmethod
    def web_url(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = urlsplit(value)
        except ValueError:
            return None
        return value if parsed.scheme in {"http", "https"} and parsed.netloc else None

    @classmethod
    def people(cls, value: Any) -> str:
        if not isinstance(value, list):
            return "Не назначены"
        names = [str(p.get("name") or p.get("username") or p.get("id") or "?")
                 for p in value if isinstance(p, dict)]
        return cls.short(", ".join(names), 400) or "Не назначены"

    @classmethod
    def project_field(cls, ctx: GitLabEventContext) -> dict:
        url = cls.web_url(ctx.project_url)
        label = cls.short(ctx.project, 400) or "unknown"
        value = f"[{label}]({url})" if url and len(label) + len(url) + 4 <= 1024 else label
        return {"name": "Проект", "value": value, "inline": True}

    @classmethod
    def branches(cls, source: Any, target: Any) -> str:
        return f"`{cls.short(source, 450) or '?'}` → `{cls.short(target, 450) or '?'}`"

    @classmethod
    def payload(cls, ctx: GitLabEventContext, *, title: str, description: str = "", url=None,
                fields: list[dict] | None = None, color: int = 0x95A5A6) -> dict:
        # Apply individual limits and the shared 6000-character budget in one place.
        title = cls.short(title, 256) or "GitLab"
        description = cls.short(description, 1800)
        author = {"name": cls.short(ctx.user_name, 256) or "someone"}
        if avatar := cls.web_url(ctx.user_avatar):
            author["icon_url"] = avatar
        embed = {
            "title": title, "color": color, "timestamp": datetime.now(timezone.utc).isoformat(),
            "author": author,
        }
        if description:
            embed["description"] = description
        if target := cls.web_url(url):
            embed["url"] = target
        remaining = 6000 - len(title) - len(description) - len(author["name"])
        limited_fields = []
        for field in (fields or [])[:25]:
            if remaining < 2:
                break
            name = cls.short(field.get("name"), min(256, remaining - 1)) or "—"
            value = cls.short(field.get("value"), min(1024, remaining - len(name))) or "—"
            limited_fields.append({"name": name, "value": value, "inline": bool(field.get("inline"))})
            remaining -= len(name) + len(value)
        if limited_fields:
            embed["fields"] = limited_fields
        return {"content": None, "embeds": [embed], "allowed_mentions": {"parse": []}}


class MergeRequestHandler:
    ACTIONS = {
        "open": "MR открыт", "reopen": "MR переоткрыт", "update": "MR обновлён",
        "approval": "Добавлено одобрение", "approved": "Получены все необходимые одобрения",
        "unapproval": "Снято одобрение", "unapproved": "MR потерял необходимое одобрение",
        "merge": "MR смержен", "close": "MR закрыт",
    }
    CHANGE_NAMES = {
        "title": "Заголовок", "source_branch": "Исходная ветка", "target_branch": "Целевая ветка",
        "reviewers": "Ревьюеры", "assignees": "Ответственные", "labels": "Метки",
        "milestone": "Milestone", "state": "Статус", "detailed_merge_status": "Готовность к слиянию",
    }

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Merge Request Hook"

    @staticmethod
    def _value(key: str, value: Any) -> str:
        if key in {"reviewers", "assignees"}:
            return Formatter.people(value)
        if key == "labels" and isinstance(value, list):
            value = ", ".join(str(v.get("title", "?")) if isinstance(v, dict) else str(v) for v in value)
        if isinstance(value, dict):
            value = value.get("title") or value.get("name") or "—"
        return Formatter.short(value, 200) or "—"

    @classmethod
    def changes(cls, ctx: GitLabEventContext) -> list[str]:
        attrs = ctx.payload["object_attributes"]
        lines = []
        for key, change in (ctx.payload.get("changes") or {}).items():
            old, new = change.get("previous"), change.get("current")
            if old == new:
                continue
            if key in {"draft", "work_in_progress"}:
                lines.append("Переведён в черновик" if new else "Снят Draft: готов к ревью")
            elif key == "description":
                lines.append("Обновлено описание MR")
            elif key == "blocking_discussions_resolved":
                lines.append("Все обсуждения решены" if new else "Есть нерешённые обсуждения")
            elif key == "merge_when_pipeline_succeeds":
                lines.append("Автослияние включено" if new else "Автослияние отключено")
            elif key in cls.CHANGE_NAMES:
                lines.append(f"{cls.CHANGE_NAMES[key]}: {cls._value(key, old)} → {cls._value(key, new)}")
            elif key not in {"updated_at", "updated_by_id", "last_edited_at", "last_edited_by_id"}:
                lines.append(f"Изменено поле «{Formatter.short(key, 80)}»")
        if attrs.get("oldrev"):
            commit = attrs.get("last_commit") or {}
            sha = (commit.get("id") or "")[:8]
            message = (commit.get("message") or commit.get("title") or "").split("\n")[0]
            lines.append(f"Новый коммит `{sha}`: {Formatter.short(message, 200)}")
        return list(dict.fromkeys(lines))

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any]:
        attrs = ctx.payload["object_attributes"]
        action = attrs["action"]
        state = attrs.get("state") or {"merge": "merged", "close": "closed"}.get(action, "opened")
        state_name = {"opened": "Открыт", "closed": "Закрыт", "merged": "Смёржен", "locked": "Заблокирован"}.get(state, state)
        draft = attrs.get("draft") if attrs.get("draft") is not None else attrs.get("work_in_progress", False)
        fields = [
            Formatter.project_field(ctx),
            {"name": "Ветки", "value": Formatter.branches(attrs.get('source_branch'), attrs.get('target_branch')), "inline": True},
            {"name": "Статус", "value": state_name, "inline": True},
            {"name": "Готовность", "value": "Черновик" if draft else "Готов к ревью", "inline": True},
        ]
        for key, name in (("reviewers", "Ревьюеры"), ("assignees", "Ответственные")):
            people = ctx.payload.get(key)
            change = (ctx.payload.get("changes") or {}).get(key)
            if people is None and change:
                people = change.get("current")
            fields.append({"name": name, "value": Formatter.people(people), "inline": True})
        summary = self.ACTIONS.get(action, f"Событие MR: {Formatter.short(action, 80)}")
        details = self.changes(ctx)
        fields.append({"name": "Последнее изменение", "value": "\n".join([summary, *details]), "inline": False})
        color = {"merged": 0x2ECC71, "closed": 0xE74C3C}.get(state, 0x95A5A6 if draft else 0x3498DB)
        return Formatter.payload(
            ctx, title=f"!{attrs['iid']} • {attrs.get('title') or 'Merge Request'}",
            url=attrs.get("url") or ctx.project_url, fields=fields, color=color,
        )


class PushHandler:
    def __init__(self, cfg: RelayConfig):
        self._cfg = cfg

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Push Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        ref = ctx.payload["ref"]
        if self._cfg.allowed_push_refs and ref not in self._cfg.allowed_push_refs:
            return None
        commits = ctx.payload.get("commits") or []
        count = ctx.payload.get("total_commits_count")
        if count is None:
            count = len(commits)
        lines = [f"`{(c.get('id') or '')[:8]}` {(c.get('message') or '').splitlines()[0] if c.get('message') else ''}" for c in commits[:5]]
        if count > len(commits[:5]):
            lines.append(f"_ещё {count - len(commits[:5])}…_")
        before, after = ctx.payload.get("before") or "", ctx.payload.get("after") or ""
        return Formatter.payload(
            ctx, title=f"Push в `{ctx.project}`", description="\n".join(lines) or "Без списка коммитов",
            url=ctx.payload.get("compare") or ctx.project_url, color=0x1ABC9C,
            fields=[
                {"name": "Ветка", "value": f"`{ref.removeprefix('refs/heads/')}`", "inline": True},
                {"name": "Коммиты", "value": str(count), "inline": True},
                {"name": "Diff", "value": f"`{before[:8]}` → `{after[:8]}`", "inline": True},
            ],
        )


class NoteHandler:
    def __init__(self, cfg: RelayConfig):
        self._cfg = cfg

    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Note Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        attrs = ctx.payload["object_attributes"]
        if self._cfg.ignore_system_notes and attrs.get("system"):
            return None
        kind = attrs["noteable_type"].lower()
        if self._cfg.only_mr_notes and kind != "mergerequest":
            return None
        mr, issue, commit = (ctx.payload.get(k) or {} for k in ("merge_request", "issue", "commit"))
        target = mr or issue or commit
        title = {"mergerequest": "Комментарий к MR", "issue": "Комментарий к Issue", "commit": "Комментарий к Commit", "snippet": "Комментарий к Snippet"}.get(kind, "Комментарий")
        fields = [
            {"name": "Тема", "value": Formatter.short(target.get("title") or target.get("message") or "Объект", 512), "inline": False},
            Formatter.project_field(ctx),
        ]
        if mr.get("source_branch") or mr.get("target_branch"):
            fields.append({"name": "Ветки", "value": Formatter.branches(mr.get('source_branch'), mr.get('target_branch')), "inline": True})
        if commit.get("id"):
            fields.append({"name": "Commit", "value": str(commit["id"])[:8], "inline": True})
        return Formatter.payload(
            ctx, title=title, description=attrs.get("note") or "", color=0x9B59B6,
            url=attrs.get("url") or target.get("url") or ctx.project_url, fields=fields,
        )


class WikiHandler:
    def can_handle(self, ctx: GitLabEventContext) -> bool:
        return ctx.event == "Wiki Page Hook"

    async def handle(self, ctx: GitLabEventContext) -> dict[str, Any]:
        attrs = ctx.payload["object_attributes"]
        return Formatter.payload(
            ctx, title=f"Wiki {attrs.get('action') or 'updated'}: {attrs.get('title') or attrs.get('slug') or 'Wiki'}",
            description=attrs.get("message") or "", url=attrs.get("url") or ctx.project_url,
            fields=[Formatter.project_field(ctx)], color=0xF39C12,
        )


class EventRouter:
    def __init__(self, handlers: list[IGitLabEventHandler]):
        self._handlers = handlers

    async def route(self, ctx: GitLabEventContext) -> dict[str, Any] | None:
        for handler in self._handlers:
            if handler.can_handle(ctx):
                return await handler.handle(ctx)
        return None
