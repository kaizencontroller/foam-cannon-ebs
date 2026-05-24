import os
import time
from typing import Any

import httpx


TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "").strip()
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "").strip()

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
HELIX_USERS_URL = "https://api.twitch.tv/helix/users"
HELIX_STREAMS_URL = "https://api.twitch.tv/helix/streams"

_app_token: str | None = None
_app_token_expires_at: float = 0


def twitch_configured() -> bool:
    return bool(TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET)


async def get_app_access_token() -> str | None:
    global _app_token
    global _app_token_expires_at

    if not twitch_configured():
        return None

    now = time.time()

    if _app_token and now < (_app_token_expires_at - 300):
        return _app_token

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            TOKEN_URL,
            data={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )

        response.raise_for_status()
        data = response.json()

    _app_token = data["access_token"]
    _app_token_expires_at = now + int(data.get("expires_in", 3600))

    return _app_token


async def twitch_get(path_url: str, params: dict[str, Any]) -> dict[str, Any]:
    token = await get_app_access_token()

    if not token:
        raise RuntimeError("Twitch API is not configured. Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET.")

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            path_url,
            headers={
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
            },
            params=params,
        )

        response.raise_for_status()
        return response.json()


async def get_user_by_login(login: str) -> dict[str, Any] | None:
    if not login:
        return None

    data = await twitch_get(HELIX_USERS_URL, {"login": login})
    users = data.get("data", [])

    if not users:
        return None

    return users[0]


async def get_stream_status_by_user_id(user_id: str) -> dict[str, Any]:
    data = await twitch_get(HELIX_STREAMS_URL, {"user_id": user_id})
    streams = data.get("data", [])

    if not streams:
        return {
            "is_live": False,
            "stream": None,
        }

    return {
        "is_live": True,
        "stream": streams[0],
    }