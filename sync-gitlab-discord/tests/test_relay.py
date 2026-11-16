import asyncio
import copy
import json
import logging
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app import JsonLogFormatter, create_app
from config import RelayConfig
from discord_client import DiscordClient
from handlers import Formatter, GitLabEventContext

WEBHOOK = "https://discord.com/api/webhooks/123456/test-webhook-token"
PROJECT = {"id": 7, "path_with_namespace": "group/project", "web_url": "https://gitlab.example/group/project"}


def mr_event(action="open", **attributes):
    attrs = {
        "iid": 12, "action": action, "title": "Improve movement", "state": "opened",
        "source_branch": "feature/movement", "target_branch": "main", "draft": True,
        "url": "https://gitlab.example/group/project/-/merge_requests/12",
        "updated_at": "2026-11-16T10:00:00Z",
    }
    attrs.update(attributes)
    return {"project": copy.deepcopy(PROJECT), "user": {"name": "Developer"},
            "object_attributes": attrs, "changes": {}, "reviewers": [], "assignees": []}


def push_event(ref="refs/heads/main", count=35):
    return {"project": copy.deepcopy(PROJECT), "user_name": "Developer", "ref": ref,
            "before": "a" * 40, "after": "b" * 40, "total_commits_count": count,
            "commits": [{"id": f"{i:040x}", "message": f"Commit {i}\nDetails"} for i in range(20)]}


def note_event(kind="MergeRequest", system=False):
    return {"project": copy.deepcopy(PROJECT), "user": {"name": "Reviewer"},
            "object_attributes": {"noteable_type": kind, "note": "Please fix this", "system": system,
                                  "url": "https://gitlab.example/group/project/-/merge_requests/12#note_99"},
            "merge_request": {"title": "Improve movement", "url": "https://gitlab.example/group/project/-/merge_requests/12"}}


class DiscordStub:
    def __init__(self):
        self.calls = []
        self.responses = []
        self.next_id = 1000

    async def __call__(self, request):
        self.calls.append({"method": request.method, "path": request.url.path,
                           "query": dict(request.url.params), "payload": json.loads(request.content)})
        await asyncio.sleep(0.005)
        if self.responses:
            answer = self.responses.pop(0)
            if isinstance(answer, Exception):
                raise answer
            status, body = answer
            return httpx.Response(status, json=body)
        if request.method == "POST":
            self.next_id += 1
            message_id = str(self.next_id)
        else:
            message_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={"id": message_id})


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = RelayConfig(WEBHOOK, allowed_push_refs=frozenset({"refs/heads/main"}),
                                  state_db_path=str(Path(self.temp.name) / "relay.sqlite3"), log_level="CRITICAL")
        self.discord = DiscordStub()

    def client(self, config=None):
        return TestClient(create_app(config or self.config, discord_transport=httpx.MockTransport(self.discord)))

    def post(self, client, payload, event="Merge Request Hook", **headers):
        return client.post("/gitlab", json=payload, headers={"X-Gitlab-Event": event, **headers})

    @staticmethod
    def fields(call):
        return {field["name"]: field["value"] for field in call["payload"]["embeds"][0].get("fields", [])}

    def test_mr_is_created_then_edited(self):
        with self.client() as client:
            self.assertEqual(self.post(client, mr_event()).json()["action"], "mr_created")
            result = self.post(client, mr_event("merge", state="merged", updated_at="2026-11-16T10:01:00Z"))
            self.assertEqual(result.json()["action"], "mr_updated")
        self.assertEqual([call["method"] for call in self.discord.calls], ["POST", "PATCH"])
        self.assertEqual(self.discord.calls[0]["query"]["wait"], "true")
        self.assertTrue(self.discord.calls[1]["path"].endswith("/messages/1001"))
        self.assertEqual(self.fields(self.discord.calls[1])["Статус"], "Смёржен")

    def test_restart_preserves_message_id(self):
        with self.client() as client:
            self.post(client, mr_event())
        with self.client() as client:
            result = self.post(client, mr_event("close", state="closed"))
            self.assertEqual(result.json()["action"], "mr_updated")
        self.assertEqual([call["method"] for call in self.discord.calls], ["POST", "PATCH"])

    def test_concurrent_events_create_one_card(self):
        with self.client() as client:
            with ThreadPoolExecutor(max_workers=6) as pool:
                responses = list(pool.map(lambda _: self.post(client, mr_event()), range(6)))
        self.assertTrue(all(r.status_code == 200 for r in responses))
        self.assertEqual(sum(c["method"] == "POST" for c in self.discord.calls), 1)
        self.assertEqual(sum(c["method"] == "PATCH" for c in self.discord.calls), 5)

    def test_same_iid_in_different_projects_has_separate_cards(self):
        another = mr_event()
        another["project"]["id"] = 8
        with self.client() as client:
            self.post(client, mr_event())
            self.post(client, another)
        self.assertEqual([c["method"] for c in self.discord.calls], ["POST", "POST"])

    def test_webhook_token_rotation_keeps_existing_card(self):
        with self.client() as client:
            self.post(client, mr_event())
        config = replace(self.config, discord_webhook_url=WEBHOOK.replace("test-webhook-token", "rotated-token"))
        with self.client(config) as client:
            self.post(client, mr_event("update"))
        self.assertEqual(self.discord.calls[-1]["method"], "PATCH")

    def test_new_webhook_has_separate_mapping(self):
        with self.client() as client:
            self.post(client, mr_event())
        with self.client(replace(self.config, discord_webhook_url=WEBHOOK.replace("123456", "987654"))) as client:
            self.post(client, mr_event())
        self.assertEqual(self.discord.calls[-1]["method"], "POST")

    def test_thread_is_preserved_on_create_and_edit(self):
        with self.client(replace(self.config, discord_webhook_url=WEBHOOK + "?thread_id=456&wait=false")) as client:
            self.post(client, mr_event())
            self.post(client, mr_event("update"))
        self.assertEqual(self.discord.calls[0]["query"], {"thread_id": "456", "wait": "true"})
        self.assertEqual(self.discord.calls[1]["query"], {"thread_id": "456"})

    def test_deleted_message_is_recreated(self):
        with self.client() as client:
            self.post(client, mr_event())
            self.discord.responses = [(404, {"code": 10008, "message": "Unknown Message"})]
            response = self.post(client, mr_event("update"))
            self.assertEqual(response.json()["action"], "mr_recreated")
            self.post(client, mr_event("update"))
        self.assertEqual([c["method"] for c in self.discord.calls], ["POST", "PATCH", "POST", "PATCH"])
        self.assertTrue(self.discord.calls[-1]["path"].endswith("/messages/1002"))

    def test_missing_webhook_does_not_trigger_recreation(self):
        with self.client() as client:
            self.post(client, mr_event())
            self.discord.responses = [(404, {"code": 10015})]
            response = self.post(client, mr_event("update"))
        self.assertEqual(response.status_code, 502)
        self.assertEqual(len(self.discord.calls), 2)

    def test_older_event_does_not_revert_card(self):
        with self.client() as client:
            self.post(client, mr_event("merge", state="merged", updated_at="2026-11-16T10:05:00Z"))
            response = self.post(client, mr_event("open"))
        self.assertEqual(response.json()["reason"], "stale_mr_event")
        self.assertEqual(len(self.discord.calls), 1)

    def test_legacy_gitlab_timestamp_is_ordered(self):
        with self.client() as client:
            self.post(client, mr_event(updated_at="2026-11-16 10:05:00 UTC"))
            response = self.post(client, mr_event(updated_at="2026-11-16 10:00:00 UTC"))
        self.assertTrue(response.json()["ignored"])

    def test_meaningful_mr_changes_and_current_reviewers(self):
        payload = mr_event("update", draft=False)
        payload["reviewers"] = [{"name": "New Reviewer"}]
        payload["changes"] = {
            "reviewers": {"previous": [{"name": "Old Reviewer"}], "current": [{"name": "New Reviewer"}]},
            "draft": {"previous": True, "current": False},
            "title": {"previous": "Old title", "current": "Improve movement"},
            "target_branch": {"previous": "develop", "current": "main"},
        }
        with self.client() as client:
            self.assertEqual(self.post(client, payload).status_code, 200)
        fields = self.fields(self.discord.calls[-1])
        self.assertEqual(fields["Ревьюеры"], "New Reviewer")
        for expected in ["Old Reviewer → New Reviewer", "Снят Draft", "Old title → Improve movement", "develop → main"]:
            self.assertIn(expected, fields["Последнее изменение"])

    def test_individual_and_complete_approval_are_distinct(self):
        with self.client() as client:
            self.post(client, mr_event("approval"))
            first = self.fields(self.discord.calls[-1])["Последнее изменение"]
            self.post(client, mr_event("approved"))
            second = self.fields(self.discord.calls[-1])["Последнее изменение"]
        self.assertIn("Добавлено одобрение", first)
        self.assertIn("все необходимые одобрения", second)

    def test_new_commit_is_described(self):
        payload = mr_event("update", oldrev="a" * 40, last_commit={"id": "b" * 40, "message": "Fix collision\nDetails"})
        with self.client() as client:
            self.post(client, payload)
        self.assertIn("Новый коммит `bbbbbbbb`: Fix collision", self.fields(self.discord.calls[-1])["Последнее изменение"])

    def test_push_uses_total_count_and_correct_remainder(self):
        with self.client() as client:
            self.post(client, push_event(), "Push Hook")
        self.assertEqual(self.fields(self.discord.calls[0])["Коммиты"], "35")
        self.assertIn("ещё 30", self.discord.calls[0]["payload"]["embeds"][0]["description"])

    def test_push_falls_back_to_commit_list_count(self):
        payload = push_event()
        del payload["total_commits_count"]
        with self.client() as client:
            self.post(client, payload, "Push Hook")
        self.assertEqual(self.fields(self.discord.calls[0])["Коммиты"], "20")

    def test_long_project_and_branch_names_fit_discord_fields(self):
        payload = mr_event(title="t" * 1000, source_branch="s" * 2000, target_branch="d" * 2000)
        payload["project"]["path_with_namespace"] = "group/" * 200
        payload["project"]["web_url"] = "https://gitlab.example/" + "p" * 2000
        with self.client() as client:
            self.assertEqual(self.post(client, payload).status_code, 200)
        embed = self.discord.calls[-1]["payload"]["embeds"][0]
        fields = self.fields(self.discord.calls[-1])
        self.assertLessEqual(len(embed["title"]), 256)
        self.assertLessEqual(len(fields["Ветки"]), 1024)
        self.assertEqual(fields["Ветки"].count("`"), 4)
        self.assertNotIn("](", fields["Проект"])

    def test_push_branch_filter(self):
        with self.client() as client:
            self.assertTrue(self.post(client, push_event("refs/heads/other"), "Push Hook").json()["ignored"])
        self.assertEqual(self.discord.calls, [])

    def test_empty_branch_filter_allows_all(self):
        with self.client(replace(self.config, allowed_push_refs=frozenset())) as client:
            self.assertEqual(self.post(client, push_event("refs/heads/other"), "Push Hook").status_code, 200)
        self.assertEqual(len(self.discord.calls), 1)

    def test_comment_keeps_anchor_and_does_not_overwrite_mr_card(self):
        with self.client() as client:
            self.post(client, mr_event())
            self.post(client, note_event(), "Note Hook")
        self.assertEqual([c["method"] for c in self.discord.calls], ["POST", "POST"])
        self.assertTrue(self.discord.calls[-1]["payload"]["embeds"][0]["url"].endswith("#note_99"))

    def test_note_filters(self):
        with self.client() as client:
            for payload in [note_event(system=True), note_event(kind="Issue")]:
                self.assertTrue(self.post(client, payload, "Note Hook").json()["ignored"])
        self.assertEqual(self.discord.calls, [])

    def test_note_filters_can_be_disabled(self):
        with self.client(replace(self.config, only_mr_notes=False, ignore_system_notes=False)) as client:
            self.post(client, note_event(kind="Issue", system=True), "Note Hook")
        self.assertEqual(len(self.discord.calls), 1)

    def test_wiki_event(self):
        payload = {"project": PROJECT, "object_attributes": {"title": "Design", "action": "update", "message": "New rules"}}
        with self.client() as client:
            self.assertEqual(self.post(client, payload, "Wiki Page Hook").status_code, 200)
        self.assertIn("Design", self.discord.calls[0]["payload"]["embeds"][0]["title"])

    def test_invalid_json_and_payload_shapes_are_client_errors(self):
        with self.client() as client:
            self.assertEqual(client.post("/gitlab", content="{", headers={"X-Gitlab-Event": "Push Hook"}).status_code, 400)
            bad = [[], {"project": PROJECT, "object_attributes": 123},
                   {"project": PROJECT, "object_attributes": {"iid": 0, "action": "open"}},
                   {"project": PROJECT, "object_attributes": {"iid": 12, "action": "open"}, "changes": {"title": []}}]
            for payload in bad:
                with self.subTest(payload=payload):
                    self.assertEqual(self.post(client, payload).status_code, 422)
        self.assertEqual(self.discord.calls, [])

    def test_validation_errors_do_not_echo_input(self):
        with self.client() as client:
            response = self.post(client, {"project": PROJECT, "object_attributes": "private-input-secret"})
        self.assertNotIn("private-input-secret", response.text)

    def test_unsupported_event_is_ignored(self):
        with self.client() as client:
            response = self.post(client, {"unfamiliar": "payload"}, "Pipeline Hook")
        self.assertEqual(response.json(), {"ok": True, "ignored": True})
        self.assertEqual(self.discord.calls, [])

    def test_discord_errors_are_sanitized_and_failed_create_is_not_saved(self):
        for failure in [(429, {"message": WEBHOOK, "retry_after": 2}), (500, {"message": WEBHOOK}),
                        httpx.ReadTimeout(WEBHOOK), httpx.ConnectError(WEBHOOK)]:
            with self.subTest(failure=type(failure).__name__):
                self.discord.responses = [failure]
                with self.client() as client:
                    with self.assertLogs("gitlab_discord", level="INFO") as captured:
                        response = self.post(client, mr_event(), **{"Idempotency-Key": "delivery-123"})
                    self.assertEqual(response.status_code, 502)
                    output = "\n".join(JsonLogFormatter().format(r) for r in captured.records)
                    self.assertIn("delivery-123", output)
                    self.assertNotIn("test-webhook-token", output + response.text)
                    self.assertEqual(self.post(client, mr_event()).json()["action"], "mr_created")
                # Isolate subsequent subtests from this successful creation.
                Path(self.config.state_db_path).unlink()

    def test_success_without_message_id_is_not_saved(self):
        self.discord.responses = [(204, {})]
        with self.client() as client:
            self.assertEqual(self.post(client, mr_event()).status_code, 502)
            self.assertEqual(self.post(client, mr_event()).json()["action"], "mr_created")

    def test_health_and_counters(self):
        with self.client() as client:
            self.post(client, mr_event())
            self.post(client, push_event("refs/heads/other"), "Push Hook")
            result = client.get("/health")
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["counters"]["received"], 2)
            self.assertEqual(result.json()["counters"]["delivered"], 1)
            self.assertEqual(result.json()["counters"]["ignored"], 1)
            self.assertNotIn("test-webhook-token", result.text)

    def test_unavailable_state_reports_503(self):
        with self.client() as client:
            # Invoke on the app's event-loop thread to respect SQLite thread ownership.
            client.portal.call(client.app.state.store.close)
            self.assertEqual(client.get("/health").status_code, 503)
            self.assertEqual(self.post(client, mr_event()).status_code, 503)


class FormattingAndConfigTests(unittest.TestCase):
    def test_all_embed_limits_and_combined_budget(self):
        ctx = GitLabEventContext("Push Hook", {}, "p" * 1000, "", "u" * 1000, None)
        payload = Formatter.payload(ctx, title="t" * 1000, description="d" * 10000,
                                    fields=[{"name": "n" * 1000, "value": "v" * 10000} for _ in range(30)])
        embed = payload["embeds"][0]
        total = len(embed["title"]) + len(embed["description"]) + len(embed["author"]["name"])
        self.assertLessEqual(len(embed["title"]), 256)
        self.assertLessEqual(len(embed["author"]["name"]), 256)
        self.assertLessEqual(len(embed["description"]), 4096)
        self.assertLessEqual(len(embed["fields"]), 25)
        for field in embed["fields"]:
            self.assertTrue(1 <= len(field["name"]) <= 256)
            self.assertTrue(1 <= len(field["value"]) <= 1024)
            total += len(field["name"]) + len(field["value"])
        self.assertLessEqual(total, 6000)
        self.assertEqual(payload["allowed_mentions"], {"parse": []})

    def test_config_rejects_invalid_urls_without_echoing_them(self):
        for value in ["", "https://example.com/private-token", "http://discord.com/api/webhooks/123/private-token"]:
            with self.subTest(value=value), self.assertRaises(ValueError) as caught:
                RelayConfig(value)
            self.assertNotIn("private-token", str(caught.exception))

    def test_config_accepts_empty_branch_filter_and_rejects_invalid_flags(self):
        with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": WEBHOOK, "ALLOWED_PUSH_REFS": ""}, clear=True):
            self.assertEqual(RelayConfig.from_env().allowed_push_refs, frozenset())
        with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": WEBHOOK, "ONLY_MR_NOTES": "typo"}, clear=True):
            with self.assertRaisesRegex(ValueError, "ONLY_MR_NOTES"):
                RelayConfig.from_env()

    def test_thread_destinations_have_different_keys(self):
        # No I/O or open clients are necessary for destination-key construction.
        first = DiscordClient(WEBHOOK + "?thread_id=1", None)
        second = DiscordClient(WEBHOOK + "?thread_id=2", None)
        self.assertNotEqual(first.destination_key, second.destination_key)

    def test_destination_is_independent_of_rendering_options_and_api_version(self):
        first = DiscordClient(WEBHOOK + "?thread_id=1&wait=true", None)
        second = DiscordClient(WEBHOOK.replace("/api/", "/api/v10/") + "?with_components=false&thread_id=1", None)
        self.assertEqual(first.destination_key, second.destination_key)


if __name__ == "__main__":
    unittest.main()
