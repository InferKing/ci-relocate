import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class RelayConfig:
    discord_webhook_url: str
    gitlab_secret: str = ""
    allowed_push_refs: frozenset[str] = frozenset()
    only_mr_notes: bool = True
    ignore_system_notes: bool = True
    state_db_path: str = "data/relay-state.sqlite3"
    log_level: str = "INFO"

    def __post_init__(self):
        url = urlsplit(self.discord_webhook_url)
        parts = url.path.rstrip("/").split("/")
        if (
            url.scheme != "https"
            or url.hostname not in {
                "discord.com", "discordapp.com", "canary.discord.com", "ptb.discord.com"
            }
            or url.username or url.password or url.fragment
            or len(parts) < 5 or parts[-3] != "webhooks"
            or not parts[-2].isdigit() or not parts[-1]
        ):
            raise ValueError("DISCORD_WEBHOOK_URL must be an HTTPS Discord webhook URL")
        if not self.state_db_path.strip():
            raise ValueError("STATE_DB_PATH must not be empty")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR or CRITICAL")

    @staticmethod
    def from_env() -> "RelayConfig":
        def flag(name: str, default: str) -> bool:
            value = os.getenv(name, default).strip().lower()
            if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
                raise ValueError(f"{name} must be a boolean")
            return value in {"1", "true", "yes", "on"}

        return RelayConfig(
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
            gitlab_secret=os.getenv("GITLAB_SECRET", "").strip(),
            allowed_push_refs=frozenset(
                ref.strip() for ref in os.getenv("ALLOWED_PUSH_REFS", "").split(",") if ref.strip()
            ),
            only_mr_notes=flag("ONLY_MR_NOTES", "1"),
            ignore_system_notes=flag("IGNORE_SYSTEM_NOTES", "1"),
            state_db_path=os.getenv("STATE_DB_PATH", str(Path("data") / "relay-state.sqlite3")),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
