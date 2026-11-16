import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MessageRecord:
    message_id: str
    event_time: float | None


class MessageStore:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, timeout=5)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS mr_messages (
                destination TEXT NOT NULL,
                project TEXT NOT NULL,
                mr_iid INTEGER NOT NULL,
                message_id TEXT NOT NULL,
                event_time REAL,
                PRIMARY KEY (destination, project, mr_iid)
            )"""
        )
        self._db.commit()

    def get(self, destination: str, project: str, iid: int) -> MessageRecord | None:
        row = self._db.execute(
            "SELECT message_id, event_time FROM mr_messages WHERE destination=? AND project=? AND mr_iid=?",
            (destination, project, iid),
        ).fetchone()
        return MessageRecord(*row) if row else None

    def save(self, destination: str, project: str, iid: int, message_id: str, event_time: float | None):
        with self._db:
            self._db.execute(
                """INSERT INTO mr_messages VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(destination, project, mr_iid) DO UPDATE SET
                message_id=excluded.message_id, event_time=excluded.event_time""",
                (destination, project, iid, message_id, event_time),
            )

    def ping(self):
        self._db.execute("SELECT 1").fetchone()

    def close(self):
        self._db.close()
