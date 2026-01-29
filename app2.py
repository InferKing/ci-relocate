import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

app = FastAPI()

DB_PATH = os.getenv("SYNC_DB_PATH", "/opt/jira-gitlab-sync/sync.db")
GITLAB_BASE_URL = os.getenv("GITLAB_BASE_URL", "").rstrip("/")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID", "")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")
JIRA_SHARED_SECRET = os.getenv("JIRA_SHARED_SECRET", "")

ALLOWED_ASSIGNEES = [x.strip() for x in os.getenv("ALLOWED_ASSIGNEES", "").split(",") if x.strip()]
JIRA_TO_GITLAB_USER_ID = json.loads(os.getenv("JIRA_TO_GITLAB_USER_ID_JSON", "{}"))
DONE_STATUSES = {x.strip() for x in os.getenv("DONE_STATUSES", "Done,Closed,Resolved").split(",") if x.strip()}

LABEL_PREFIX_STATUS = os.getenv("LABEL_PREFIX_STATUS", "jira:status:")
LABEL_OUT_OF_SCOPE = os.getenv("LABEL_OUT_OF_SCOPE", "sync:out-of-scope")
LABEL_IN_SCOPE = os.getenv("LABEL_IN_SCOPE", "sync:in-scope")

SYNC_MARKER_PREFIX = os.getenv("SYNC_MARKER_PREFIX", "JIRA_KEY:")

class WebhookResponse(BaseModel):
    ok: bool
    action: str
    jira_key: Optional[str] = None
    gitlab_iid: Optional[int] = None

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS links (
            jira_key TEXT PRIMARY KEY,
            gitlab_project_id TEXT NOT NULL,
            gitlab_issue_iid INTEGER NOT NULL,
            last_synced_at TEXT NOT NULL,
            sync_state TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed (
            event_fingerprint TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        )
        """
    )
    return conn

def _fingerprint(payload: Dict[str, Any]) -> str:
    issue = payload.get("issue") or {}
    key = issue.get("key") or ""
    updated = ((issue.get("fields") or {}).get("updated")) or ""
    event_type = payload.get("webhookEvent") or payload.get("issue_event_type_name") or ""
    return f"{event_type}|{key}|{updated}"

def _is_processed(fp: str) -> bool:
    conn = _db()
    try:
        cur = conn.execute("SELECT 1 FROM processed WHERE event_fingerprint = ?", (fp,))
        return cur.fetchone() is not None
    finally:
        conn.close()

def _mark_processed(fp: str) -> None:
    conn = _db()
    try:
        conn.execute("INSERT OR IGNORE INTO processed(event_fingerprint, created_at) VALUES(?, ?)", (fp, _now_iso()))
        conn.commit()
    finally:
        conn.close()

def _get_link(jira_key: str) -> Optional[Tuple[str, int, str]]:
    conn = _db()
    try:
        cur = conn.execute(
            "SELECT gitlab_project_id, gitlab_issue_iid, sync_state FROM links WHERE jira_key = ?",
            (jira_key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0], int(row[1]), row[2]
    finally:
        conn.close()

def _upsert_link(jira_key: str, iid: int, sync_state: str) -> None:
    conn = _db()
    try:
        conn.execute(
            """
            INSERT INTO links(jira_key, gitlab_project_id, gitlab_issue_iid, last_synced_at, sync_state)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(jira_key) DO UPDATE SET
                gitlab_project_id = excluded.gitlab_project_id,
                gitlab_issue_iid = excluded.gitlab_issue_iid,
                last_synced_at = excluded.last_synced_at,
                sync_state = excluded.sync_state
            """,
            (jira_key, str(GITLAB_PROJECT_ID), int(iid), _now_iso(), sync_state),
        )
        conn.commit()
    finally:
        conn.close()

def _jira_extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    issue = payload.get("issue") or {}
    fields = issue.get("fields") or {}

    key = issue.get("key")
    summary = fields.get("summary") or ""
    desc = fields.get("description")
    if desc is None:
        desc = ""
    status = ((fields.get("status") or {}).get("name")) or ""
    assignee_obj = fields.get("assignee")
    assignee = None
    if assignee_obj:
        assignee = assignee_obj.get("name") or assignee_obj.get("key") or assignee_obj.get("accountId")

    labels = fields.get("labels") or []
    if not isinstance(labels, list):
        labels = []

    return {
        "key": key,
        "summary": summary,
        "description": desc,
        "status": status,
        "assignee": assignee,
        "labels": labels,
    }

def _build_description(jira_key: str, jira_desc: str) -> str:
    marker = f"{SYNC_MARKER_PREFIX} {jira_key}"
    if marker in jira_desc:
        return jira_desc
    if jira_desc.strip():
        return f"{jira_desc}\n\n---\n{marker}"
    return f"{marker}"

def _labels_merge(jira_labels: List[str], status: str, in_scope: bool) -> List[str]:
    out = []
    for l in jira_labels:
        if isinstance(l, str) and l.strip():
            out.append(l.strip())

    out = [l for l in out if not l.startswith(LABEL_PREFIX_STATUS)]
    out.append(f"{LABEL_PREFIX_STATUS}{status}" if status else f"{LABEL_PREFIX_STATUS}unknown")

    out = [l for l in out if l not in (LABEL_OUT_OF_SCOPE, LABEL_IN_SCOPE)]
    out.append(LABEL_IN_SCOPE if in_scope else LABEL_OUT_OF_SCOPE)

    seen = set()
    uniq = []
    for l in out:
        if l not in seen:
            uniq.append(l)
            seen.add(l)
    return uniq

def _gitlab_headers() -> Dict[str, str]:
    return {"PRIVATE-TOKEN": GITLAB_TOKEN}

def _gitlab_issue_url(iid: int) -> str:
    return f"{GITLAB_BASE_URL}/api/v4/projects/{GITLAB_PROJECT_ID}/issues/{iid}"

def _gitlab_create_url() -> str:
    return f"{GITLAB_BASE_URL}/api/v4/projects/{GITLAB_PROJECT_ID}/issues"

async def _gitlab_create_issue(title: str, description: str, labels: List[str], assignee_ids: List[int], state_event: Optional[str]) -> int:
    data: Dict[str, Any] = {
        "title": title,
        "description": description,
        "labels": ",".join(labels) if labels else "",
    }
    if assignee_ids:
        data["assignee_ids"] = assignee_ids
    if state_event:
        data["state_event"] = state_event

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(_gitlab_create_url(), headers=_gitlab_headers(), data=data)
        if r.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"GitLab create failed: {r.status_code} {r.text}")
        return int(r.json()["iid"])

async def _gitlab_update_issue(iid: int, title: str, description: str, labels: List[str], assignee_ids: List[int], state_event: Optional[str]) -> None:
    data: Dict[str, Any] = {
        "title": title,
        "description": description,
        "labels": ",".join(labels) if labels else "",
    }

    if assignee_ids:
        data["assignee_ids"] = assignee_ids
    else:
        data["assignee_ids"] = ""

    if state_event:
        data["state_event"] = state_event

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.put(_gitlab_issue_url(iid), headers=_gitlab_headers(), data=data)
        if r.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"GitLab update failed: {r.status_code} {r.text}")

def _is_in_scope(assignee: Optional[str]) -> bool:
    if not assignee:
        return False
    if not ALLOWED_ASSIGNEES:
        return True
    return assignee in ALLOWED_ASSIGNEES

def _map_assignee_to_gitlab_ids(assignee: Optional[str], in_scope: bool) -> List[int]:
    if not in_scope:
        return []
    if not assignee:
        return []
    v = JIRA_TO_GITLAB_USER_ID.get(assignee)
    if v is None:
        return []
    try:
        return [int(v)]
    except Exception:
        return []

def _state_event_from_status(status: str) -> Optional[str]:
    if not status:
        return None
    if status in DONE_STATUSES:
        return "close"
    return "reopen"

@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True}

@app.post("/jira/webhook", response_model=WebhookResponse)
async def jira_webhook(request: Request, x_sync_token: Optional[str] = Header(default=None)) -> WebhookResponse:
    if JIRA_SHARED_SECRET:
        token_ok = (x_sync_token == JIRA_SHARED_SECRET) or (request.query_params.get("token") == JIRA_SHARED_SECRET)
        if not token_ok:
            raise HTTPException(status_code=401, detail="Bad token")

    if not GITLAB_BASE_URL or not GITLAB_PROJECT_ID or not GITLAB_TOKEN:
        raise HTTPException(status_code=500, detail="GitLab env is not configured")

    payload = await request.json()
    fp = _fingerprint(payload)
    if _is_processed(fp):
        return WebhookResponse(ok=True, action="dedup")

    data = _jira_extract(payload)
    jira_key = data["key"]
    if not jira_key:
        _mark_processed(fp)
        return WebhookResponse(ok=True, action="skip_no_key")

    in_scope = _is_in_scope(data["assignee"])
    link = _get_link(jira_key)

    title = data["summary"] or jira_key
    description = _build_description(jira_key, data["description"] or "")
    state_event = _state_event_from_status(data["status"])
    labels = _labels_merge(data["labels"], data["status"], in_scope)
    assignee_ids = _map_assignee_to_gitlab_ids(data["assignee"], in_scope)

    if link is None:
        if not in_scope:
            _mark_processed(fp)
            return WebhookResponse(ok=True, action="skip_create_out_of_scope", jira_key=jira_key)

        iid = await _gitlab_create_issue(
            title=title,
            description=description,
            labels=labels,
            assignee_ids=assignee_ids,
            state_event=("close" if state_event == "close" else None),
        )
        _upsert_link(jira_key, iid, "in-scope")
        _mark_processed(fp)
        return WebhookResponse(ok=True, action="created", jira_key=jira_key, gitlab_iid=iid)

    _, iid, _ = link

    await _gitlab_update_issue(
        iid=iid,
        title=title,
        description=description,
        labels=labels,
        assignee_ids=assignee_ids,
        state_event=state_event,
    )
    _upsert_link(jira_key, iid, "in-scope" if in_scope else "out-of-scope")
    _mark_processed(fp)
    return WebhookResponse(ok=True, action="updated", jira_key=jira_key, gitlab_iid=iid)
