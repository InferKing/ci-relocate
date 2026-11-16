import hashlib

import httpx


class DiscordError(Exception):
    def __init__(self, kind: str, status_code: int | None = None):
        self.kind = kind
        self.status_code = status_code
        super().__init__(kind)


class MessageNotFound(DiscordError):
    pass


class DiscordClient:
    def __init__(self, webhook_url: str, client: httpx.AsyncClient):
        self._url = httpx.URL(webhook_url.rstrip("/"))
        self._client = client
        # Token rotation must not create a second card. Threads are separate destinations.
        webhook_id = self._url.path.rstrip("/").split("/")[-2]
        destination = f"{webhook_id}:{self._url.params.get('thread_id', '')}"
        self.destination_key = hashlib.sha256(destination.encode()).hexdigest()

    async def _request(self, method: str, url: httpx.URL, payload: dict) -> dict:
        try:
            response = await self._client.request(method, url, json=payload)
        except httpx.TimeoutException:
            raise DiscordError("timeout") from None
        except httpx.RequestError:
            raise DiscordError("connection_error") from None

        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 404 and isinstance(body, dict) and body.get("code") == 10008:
            raise MessageNotFound("message_not_found", 404)
        if not 200 <= response.status_code < 300:
            raise DiscordError("http_error", response.status_code)
        if not isinstance(body, dict) or not str(body.get("id", "")).isdigit():
            raise DiscordError("invalid_response", response.status_code)
        return body

    async def create(self, payload: dict) -> str:
        body = await self._request("POST", self._url.copy_set_param("wait", "true"), payload)
        return str(body["id"])

    async def edit(self, message_id: str, payload: dict) -> None:
        url = self._url.copy_with(path=f"{self._url.path.rstrip('/')}/messages/{message_id}")
        await self._request("PATCH", url.copy_remove_param("wait"), payload)
