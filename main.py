from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path
from typing import Any
import asyncio
import base64
import os
import random
import time
import uuid
import jwt

from twitch_api import (
    twitch_configured,
    get_user_by_login,
    get_stream_status_by_user_id,
)

from persistence import (
    init_db,
    load_turret_state,
    save_turret_state,
    persist_event,
    load_recent_events as db_load_recent_events,
    persist_transaction,
    transaction_exists,
    persistence_enabled,
)

BROADCASTER_TWITCH_LOGIN = os.getenv("BROADCASTER_TWITCH_LOGIN", "").strip()
STREAM_GATE_ENABLED = os.getenv("STREAM_GATE_ENABLED", "false").strip().lower() == "true"
STREAM_STATUS_CACHE_SECONDS = int(os.getenv("STREAM_STATUS_CACHE_SECONDS", "120"))

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()
    load_all_persisted_state()

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten later for production.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEFAULT_CHANNEL_ID = "local_test"
DEFAULT_TURRET_ID = "main"



MODE_LIVE_FIRE = "live_fire"
MODE_QUEUE_ONLY = "queue_only"
MODE_DISABLED = "disabled"

PAYMENT_FREE_TEST = "free_test"
PAYMENT_BITS = "bits"

TWITCH_EXTENSION_SECRET = os.getenv("TWITCH_EXTENSION_SECRET", "").strip()

# For production/Railway, set FOAM_CONTROL_TOKEN as an environment variable.
# The "goldfish" fallback is only for local testing.
FOAM_CONTROL_TOKEN = os.getenv("FOAM_CONTROL_TOKEN", "goldfish").strip()

MAX_EVENT_LOG = 100


channels = {
    DEFAULT_CHANNEL_ID: {
        "turrets": {
            DEFAULT_TURRET_ID: {
                "display_name": "Kaizen Foam Cannon",
                "gun_type": "Swarmfire",
                "magazine_capacity": 20,
                "channel_id": DEFAULT_CHANNEL_ID,
                "turret_id": DEFAULT_TURRET_ID,
		"twitch_login": BROADCASTER_TWITCH_LOGIN,
                "twitch_user_id": None,
                "stream_gate_enabled": STREAM_GATE_ENABLED,
                "stream_status": "unknown",
                "stream_is_live": None,
                "stream_status_checked_at": None,
                "stream_status_detail": "Stream status has not been checked yet.",

                # Physical darts assumed loaded.
                "available_shots": 20,

                # Reserved for later firing.
                "queued_shots": 0,

                # Currently being processed/fired.
                "pending_shots": 0,

                "enabled": True,
                "operation_mode": MODE_LIVE_FIRE,

                # Queue behavior
                "auto_fire_queue": True,
                "allow_overqueue": False,
                "max_queue_size": 20,

                # Random queue release behavior
                "random_queue_release": False,
                "random_min_seconds": 0,
                "random_max_seconds": 30,
                "random_burst_enabled": False,
                "random_burst_min": 1,
                "random_burst_max": 3,
                "random_fixed_batch_size": 1,
                "random_release_next_at": None,

                "is_busy": False,
                "cooldown_seconds": 0.75,
                "max_shots_per_redeem": 10,
                "streamerbot_action": "Nerf Turret",

                # Local-control pairing/security
                "control_token": FOAM_CONTROL_TOKEN,

                # Payment behavior
                "payment_mode": PAYMENT_FREE_TEST,
                "bits_products": {
                    "test_fire_1": {
                        "sku": "test_fire_1",
                        "name": "Test Fire 1 Dart",
                        "shots": 1,
                        "bits": 1,
                        "in_development": True,
                    },
                    "test_fire_3": {
                        "sku": "test_fire_3",
                        "name": "Test Fire 3 Darts",
                        "shots": 3,
                        "bits": 2,
                        "in_development": True,
                    },
                    "test_fire_5": {
                        "sku": "test_fire_5",
                        "name": "Test Fire 5 Darts",
                        "shots": 5,
                        "bits": 3,
                        "in_development": True,
                    },
                    "test_fire_10": {
                        "sku": "test_fire_10",
                        "name": "Test Fire 10 Darts",
                        "shots": 10,
                        "bits": 5,
                        "in_development": True,
                    },
                },
                "processed_transactions": {},

                "connection_status": "offline",
                "streamerbot_status": "unknown",
                "streamerbot_detail": "No Streamer.bot health check yet.",

                "last_seen": None,
                "last_fire_result": "No shots fired yet.",
                "pending_commands": {},

                # Streamer-facing diagnostics
                "event_log": [],
            }
        }
    }
}

control_connections = {}
random_release_tasks = {}


class ShotRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    count: int


class ReloadRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    capacity: int = 20


class TurretActionRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID


class ModeRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    operation_mode: str


class FireQueueRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    count: int | None = None


class AutoFireQueueRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    auto_fire_queue: bool


class QueueSettingsRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    allow_overqueue: bool
    max_queue_size: int
    random_queue_release: bool
    random_min_seconds: float = 0
    random_max_seconds: float = 30
    random_burst_enabled: bool = False
    random_burst_min: int = 1
    random_burst_max: int = 3
    random_fixed_batch_size: int = 1


class PaymentSettingsRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    payment_mode: str


class BitsTransactionRequest(BaseModel):
    channel_id: str = DEFAULT_CHANNEL_ID
    turret_id: str = DEFAULT_TURRET_ID
    sku: str
    transaction_id: str
    user_id: str | None = None
    user_name: str | None = None
    product_cost: int | None = None
    transaction_receipt: dict[str, Any] | None = None


def log_event(
    turret,
    event_type: str,
    message: str,
    level: str = "info",
    extra: dict[str, Any] | None = None,
):
    if extra is None:
        extra = {}

    event = {
        "timestamp": time.time(),
        "event_type": event_type,
        "level": level,
        "message": message,
        "extra": extra,
    }

    turret.setdefault("event_log", []).insert(0, event)
    turret["event_log"] = turret["event_log"][:MAX_EVENT_LOG]

    channel_id = turret.get("channel_id", DEFAULT_CHANNEL_ID)
    turret_id = turret.get("turret_id", DEFAULT_TURRET_ID)

    persist_event(
        channel_id=channel_id,
        turret_id=turret_id,
        event_type=event_type,
        level=level,
        message=message,
        extra=extra,
        event_timestamp=event["timestamp"],
    )


def get_recent_events(turret, limit: int = 25):
    channel_id = turret.get("channel_id", DEFAULT_CHANNEL_ID)
    turret_id = turret.get("turret_id", DEFAULT_TURRET_ID)

    if persistence_enabled():
        events = db_load_recent_events(channel_id, turret_id, limit)
        if events:
            return events

    return turret.get("event_log", [])[:limit]

PERSISTED_STATE_KEYS = [
    "display_name",
    "gun_type",
    "magazine_capacity",
    "available_shots",
    "queued_shots",
    "operation_mode",
    "auto_fire_queue",
    "allow_overqueue",
    "max_queue_size",
    "random_queue_release",
    "random_min_seconds",
    "random_max_seconds",
    "random_burst_enabled",
    "random_burst_min",
    "random_burst_max",
    "random_fixed_batch_size",
    "cooldown_seconds",
    "max_shots_per_redeem",
    "streamerbot_action",
    "payment_mode",
    "bits_products",
    "twitch_login",
    "twitch_user_id",
    "stream_gate_enabled",
]


def get_state_snapshot(turret):
    snapshot = {}

    for key in PERSISTED_STATE_KEYS:
        if key in turret:
            snapshot[key] = turret[key]

    return snapshot


def save_state_for_turret(turret):
    channel_id = turret.get("channel_id", DEFAULT_CHANNEL_ID)
    turret_id = turret.get("turret_id", DEFAULT_TURRET_ID)

    save_turret_state(
        channel_id=channel_id,
        turret_id=turret_id,
        state=get_state_snapshot(turret),
    )


def apply_persisted_state(turret, persisted_state):
    if not persisted_state:
        return

    for key in PERSISTED_STATE_KEYS:
        if key in persisted_state:
            turret[key] = persisted_state[key]

    # Safety reset: never restore volatile live state as if it were still true.
    turret["connection_status"] = "offline"
    turret["streamerbot_status"] = "unknown"
    turret["streamerbot_detail"] = "Waiting for local control client after EBS restart."
    turret["is_busy"] = False
    turret["pending_shots"] = 0
    turret["pending_commands"] = {}
    turret["random_release_next_at"] = None

    # Safety choice: after an EBS restart, do not auto-arm.
    if turret["operation_mode"] != MODE_DISABLED:
        turret["operation_mode"] = MODE_DISABLED
        turret["enabled"] = False
        turret["last_fire_result"] = "State restored after restart. Cannon disabled for safety."


def load_all_persisted_state():
    for channel_id, channel_data in channels.items():
        for turret_id, turret in channel_data.get("turrets", {}).items():
            turret["channel_id"] = channel_id
            turret["turret_id"] = turret_id

            persisted_state = load_turret_state(channel_id, turret_id)

            if persisted_state:
                apply_persisted_state(turret, persisted_state)
                log_event(
                    turret,
                    "state_restored",
                    "Persisted turret state restored. Cannon disabled for safety.",
                    "success",
                )
            else:
                save_state_for_turret(turret)
                log_event(
                    turret,
                    "state_initialized",
                    "Initial turret state saved to persistence.",
                    "info",
                )

async def refresh_stream_status_for_turret(turret, force: bool = False):
    now = time.time()

    if not turret.get("stream_gate_enabled"):
        turret["stream_status"] = "disabled"
        turret["stream_is_live"] = True
        turret["stream_status_detail"] = "Stream gate disabled."
        return

    if not twitch_configured():
        turret["stream_status"] = "unknown"
        turret["stream_is_live"] = None
        turret["stream_status_detail"] = "Twitch API credentials are not configured."
        return

    last_checked = turret.get("stream_status_checked_at")

    if (
        not force
        and last_checked
        and now - float(last_checked) < STREAM_STATUS_CACHE_SECONDS
    ):
        return

    try:
        twitch_login = turret.get("twitch_login") or BROADCASTER_TWITCH_LOGIN

        if not twitch_login:
            turret["stream_status"] = "unknown"
            turret["stream_is_live"] = None
            turret["stream_status_detail"] = "No broadcaster Twitch login configured."
            turret["stream_status_checked_at"] = now
            return

        if not turret.get("twitch_user_id"):
            user = await get_user_by_login(twitch_login)

            if not user:
                turret["stream_status"] = "unknown"
                turret["stream_is_live"] = None
                turret["stream_status_detail"] = f"Twitch user not found: {twitch_login}."
                turret["stream_status_checked_at"] = now
                return

            turret["twitch_user_id"] = user["id"]
            save_state_for_turret(turret)

        status = await get_stream_status_by_user_id(turret["twitch_user_id"])

        turret["stream_is_live"] = bool(status["is_live"])
        turret["stream_status"] = "live" if status["is_live"] else "offline"
        turret["stream_status_checked_at"] = now
        turret["stream_status_detail"] = (
            "Stream is live."
            if status["is_live"]
            else "Stream is offline."
        )

    except Exception as error:
        turret["stream_status"] = "unknown"
        turret["stream_is_live"] = None
        turret["stream_status_checked_at"] = now
        turret["stream_status_detail"] = f"Stream status check failed: {str(error)}"


async def stream_gate_allows_fire(turret):
    await refresh_stream_status_for_turret(turret)

    if not turret.get("stream_gate_enabled"):
        return True, "Stream gate disabled."

    if turret.get("stream_is_live") is True:
        return True, "Stream is live."

    if turret.get("stream_is_live") is False:
        return False, "Streamer is offline. Foam Cannon is unavailable."

    return False, turret.get("stream_status_detail") or "Stream status unknown."

def decode_twitch_extension_jwt(authorization_header: str | None):
    """
    Verifies the Twitch Extension JWT if TWITCH_EXTENSION_SECRET is configured.

    For early local testing, leaving TWITCH_EXTENSION_SECRET blank disables
    enforcement. For Bits/public use, set it in Railway.
    """
    if not TWITCH_EXTENSION_SECRET:
        return {
            "ok": True,
            "enforced": False,
            "claims": None,
        }

    if not authorization_header:
        return {
            "ok": False,
            "error": "Missing Authorization header.",
        }

    if not authorization_header.lower().startswith("bearer "):
        return {
            "ok": False,
            "error": "Authorization header must use Bearer token.",
        }

    token = authorization_header.split(" ", 1)[1].strip()

    try:
        secret_bytes = base64.b64decode(TWITCH_EXTENSION_SECRET)
    except Exception:
        secret_bytes = TWITCH_EXTENSION_SECRET.encode("utf-8")

    try:
        claims = jwt.decode(
            token,
            secret_bytes,
            algorithms=["HS256"],
            options={
                "verify_aud": False,
            },
        )

        return {
            "ok": True,
            "enforced": True,
            "claims": claims,
        }

    except Exception as error:
        return {
            "ok": False,
            "error": f"Invalid Twitch extension JWT: {str(error)}",
        }


def get_turret(channel_id: str, turret_id: str):
    if channel_id not in channels:
        raise KeyError(f"Unknown channel_id: {channel_id}")

    turrets = channels[channel_id].get("turrets", {})

    if turret_id not in turrets:
        raise KeyError(f"Unknown turret_id: {turret_id}")

    return turrets[turret_id]


def get_connection_key(channel_id: str, turret_id: str):
    return f"{channel_id}:{turret_id}"


def verify_control_token(turret, provided_token: str | None):
    expected_token = turret.get("control_token", "")

    if not expected_token:
        return False, "No control token is configured on the EBS."

    if not provided_token:
        return False, "No control token was provided by local-control."

    if provided_token != expected_token:
        return False, "Invalid control token."

    return True, "Control token accepted."


def get_unreserved_shots(turret):
    return max(
        0,
        turret["available_shots"] - turret["queued_shots"] - turret["pending_shots"],
    )


def get_physical_fireable_shots(turret):
    return max(0, turret["available_shots"] - turret["pending_shots"])


def get_queue_capacity_remaining(turret):
    if turret["allow_overqueue"]:
        return max(0, turret["max_queue_size"] - turret["queued_shots"])

    return get_unreserved_shots(turret)


def is_enabled(turret):
    return turret["operation_mode"] != MODE_DISABLED


def local_system_ready(turret):
    return (
        turret["connection_status"] == "online"
        and turret["streamerbot_status"] == "online"
    )


def auto_disable_turret(turret, reason: str):
    turret["operation_mode"] = MODE_DISABLED
    turret["enabled"] = False
    turret["is_busy"] = False
    turret["pending_shots"] = 0
    turret["pending_commands"] = {}
    turret["random_release_next_at"] = None
    turret["last_fire_result"] = reason
    log_event(turret, "auto_disable", reason, "warning")
    save_state_for_turret(turret)


def get_can_fire_now(turret):
    return (
        turret["operation_mode"] == MODE_LIVE_FIRE
        and local_system_ready(turret)
        and not turret["is_busy"]
        and get_physical_fireable_shots(turret) > 0
    )


def get_can_queue(turret):
    return (
        turret["operation_mode"] in {MODE_LIVE_FIRE, MODE_QUEUE_ONLY}
        and local_system_ready(turret)
        and get_queue_capacity_remaining(turret) > 0
    )


def get_viewer_action_label(turret):
    if turret["operation_mode"] == MODE_LIVE_FIRE and not turret["is_busy"]:
        return "Fire"

    if turret["operation_mode"] in {MODE_LIVE_FIRE, MODE_QUEUE_ONLY}:
        return "Queue"

    return "Unavailable"


def get_viewer_message(turret):
    if turret["connection_status"] != "online":
        return "Local control client is offline. Fire buttons blocked."

    if turret["streamerbot_status"] != "online":
        return f"Streamer.bot is offline. {turret['streamerbot_detail']}"

    if turret["operation_mode"] == MODE_DISABLED:
        return "Foam Cannon is disabled."

    if turret["operation_mode"] == MODE_QUEUE_ONLY:
        if turret["allow_overqueue"]:
            return "Queue Mode Active. Queue may exceed loaded darts and require reloads."
        return "Queue Mode Active. Shots will fire when the streamer releases the queue."

    if turret["operation_mode"] == MODE_LIVE_FIRE and turret["is_busy"]:
        return "Cannon is firing. New requests will be queued."

    if get_queue_capacity_remaining(turret) <= 0:
        return "Foam Cannon has no queue capacity available."

    return turret["last_fire_result"] or "Ready."


def format_setup_checks(turret):
    ebs_online = True
    local_control_online = turret["connection_status"] == "online"
    streamerbot_online = turret["streamerbot_status"] == "online"
    cannon_armed = turret["operation_mode"] != MODE_DISABLED
    has_loaded_shots = turret["available_shots"] > 0
    not_busy = not turret["is_busy"]
    payment_selected = turret["payment_mode"] in {PAYMENT_FREE_TEST, PAYMENT_BITS}
    pairing_token_configured = bool(turret.get("control_token"))

    return [
        {
            "key": "ebs",
            "label": "Hosted EBS online",
            "ok": ebs_online,
            "detail": "Backend is responding.",
        },
        {
            "key": "local_control",
            "label": "Local control connected",
            "ok": local_control_online,
            "detail": "Connected." if local_control_online else "Local-control client is not connected.",
        },
        {
            "key": "streamerbot",
            "label": "Streamer.bot online",
            "ok": streamerbot_online,
            "detail": turret["streamerbot_detail"] if not streamerbot_online else "Health check accepted.",
        },
        {
            "key": "cannon_armed",
            "label": "Cannon armed",
            "ok": cannon_armed,
            "detail": f"Mode: {turret['operation_mode']}.",
        },
        {
            "key": "loaded_shots",
            "label": "Darts loaded",
            "ok": has_loaded_shots,
            "detail": f"{turret['available_shots']} loaded, {turret['queued_shots']} queued, {turret['pending_shots']} firing.",
        },
        {
            "key": "not_busy",
            "label": "Ready for command",
            "ok": not_busy,
            "detail": "Ready." if not_busy else "A fire command is currently processing.",
        },
        {
            "key": "payment_mode",
            "label": "Payment mode selected",
            "ok": payment_selected,
            "detail": f"Payment mode: {turret['payment_mode']}.",
        },
        {
            "key": "pairing_token",
            "label": "Pairing token configured",
            "ok": pairing_token_configured,
            "detail": "Configured." if pairing_token_configured else "Missing local-control pairing token.",
        },
        {
            "key": "jwt",
            "label": "JWT enforcement",
            "ok": bool(TWITCH_EXTENSION_SECRET),
            "detail": "On." if TWITCH_EXTENSION_SECRET else "Off for development/testing.",
        },
    ]


def reserve_for_fire(turret, count: int, source: str):
    if source == "live_fire":
        if count > get_physical_fireable_shots(turret):
            return False, "Not enough loaded shots available to fire."
        turret["pending_shots"] += count
        return True, None

    if source == "queue":
        if count > turret["queued_shots"]:
            return False, "Not enough queued shots available."
        if count > get_physical_fireable_shots(turret):
            return False, "Not enough loaded shots available. Reload needed."
        turret["queued_shots"] -= count
        turret["pending_shots"] += count
        return True, None

    return False, f"Unknown fire source: {source}"


def restore_failed_fire_reservation(turret, count: int, source: str):
    turret["pending_shots"] = max(0, turret["pending_shots"] - count)

    if source == "queue":
        turret["queued_shots"] += count


def add_to_queue(turret, count: int, source_label: str):
    if count <= 0:
        return {"ok": False, "error": "Queue count must be greater than zero."}

    if count > turret["max_shots_per_redeem"]:
        return {
            "ok": False,
            "error": f"Maximum shots per request is {turret['max_shots_per_redeem']}.",
        }

    if count > get_queue_capacity_remaining(turret):
        if turret["allow_overqueue"]:
            return {"ok": False, "error": "Queue limit reached."}
        return {"ok": False, "error": "Not enough unreserved loaded shots available to queue."}

    turret["queued_shots"] += count
    turret["last_fire_result"] = f"{source_label}: queued {count} shot(s)."
    log_event(
        turret,
        "queue_add",
        f"{source_label}: queued {count} shot(s).",
        "info",
        {"count": count, "source": source_label},
    )
    save_state_for_turret(turret)

    return {
        "ok": True,
        "message": f"Queued {count} shot(s).",
        "available_shots": turret["available_shots"],
        "queued_shots": turret["queued_shots"],
        "pending_shots": turret["pending_shots"],
        "unreserved_shots": get_unreserved_shots(turret),
        "queue_capacity_remaining": get_queue_capacity_remaining(turret),
    }


async def send_fire_command(channel_id: str, turret_id: str, count: int, source: str):
    turret = get_turret(channel_id, turret_id)
    connection_key = get_connection_key(channel_id, turret_id)
    websocket = control_connections.get(connection_key)

    if websocket is None:
        turret["connection_status"] = "offline"
        auto_disable_turret(
            turret,
            "Auto-disabled because no active local control connection was found.",
        )
        return {
            "ok": False,
            "error": "No active local control connection.",
        }

    if turret["streamerbot_status"] != "online":
        auto_disable_turret(
            turret,
            f"Auto-disabled because Streamer.bot is offline. {turret['streamerbot_detail']}",
        )
        return {
            "ok": False,
            "error": f"Streamer.bot is offline. {turret['streamerbot_detail']}",
        }

    if turret["is_busy"]:
        return {
            "ok": False,
            "error": "Turret is already processing another fire command.",
        }

    reserved_ok, reserve_error = reserve_for_fire(turret, count, source)

    if not reserved_ok:
        log_event(
            turret,
            "fire_blocked",
            reserve_error,
            "warning",
            {"count": count, "source": source},
        )
        return {
            "ok": False,
            "error": reserve_error,
        }

    command_id = str(uuid.uuid4())

    command = {
        "type": "fire",
        "command_id": command_id,
        "channel_id": channel_id,
        "turret_id": turret_id,
        "shots": count,
        "streamerbot_action": turret["streamerbot_action"],
        "cooldown_seconds": turret["cooldown_seconds"],
    }

    try:
        turret["is_busy"] = True
        turret["random_release_next_at"] = None
        turret["pending_commands"][command_id] = {
            "source": source,
            "count": count,
            "created_at": time.time(),
        }
        turret["last_fire_result"] = f"Processing command {command_id} for {count} shot(s)."

        log_event(
            turret,
            "fire_sent",
            f"Sent {count} shot(s) to local-control.",
            "info",
            {"command_id": command_id, "count": count, "source": source},
        )

        await websocket.send_json(command)

        return {
            "ok": True,
            "message": f"Sent {count} shot(s) to local control client.",
            "command_id": command_id,
            "available_shots": turret["available_shots"],
            "queued_shots": turret["queued_shots"],
            "pending_shots": turret["pending_shots"],
            "unreserved_shots": get_unreserved_shots(turret),
            "queue_capacity_remaining": get_queue_capacity_remaining(turret),
            "is_busy": turret["is_busy"],
        }

    except Exception as error:
        turret["pending_commands"].pop(command_id, None)
        restore_failed_fire_reservation(turret, count, source)
        turret["connection_status"] = "offline"
        auto_disable_turret(
            turret,
            f"Auto-disabled because sending the fire command failed: {str(error)}",
        )
        return {
            "ok": False,
            "error": f"Failed to send fire command: {str(error)}",
        }


def get_random_burst_count(turret):
    if turret["random_burst_enabled"]:
        low = max(1, int(turret["random_burst_min"]))
        high = max(low, int(turret["random_burst_max"]))
        return random.randint(low, high)

    return max(1, int(turret["random_fixed_batch_size"]))


async def random_release_worker(channel_id: str, turret_id: str):
    key = get_connection_key(channel_id, turret_id)

    try:
        turret = get_turret(channel_id, turret_id)

        delay = random.uniform(
            float(turret["random_min_seconds"]),
            float(turret["random_max_seconds"]),
        )

        turret["random_release_next_at"] = time.time() + delay
        turret["last_fire_result"] = f"Random queue release scheduled in {delay:.1f} second(s)."
        log_event(
            turret,
            "random_release_scheduled",
            f"Random queue release scheduled in {delay:.1f} second(s).",
            "info",
            {"delay": delay},
        )

        await asyncio.sleep(delay)

        turret = get_turret(channel_id, turret_id)

        if not turret["random_queue_release"]:
            turret["random_release_next_at"] = None
            return

        if turret["operation_mode"] != MODE_LIVE_FIRE:
            turret["random_release_next_at"] = None
            return

        if not local_system_ready(turret):
            turret["random_release_next_at"] = None
            return

        if turret["is_busy"]:
            turret["random_release_next_at"] = None
            schedule_random_release_if_needed(channel_id, turret_id)
            return

        if turret["queued_shots"] <= 0:
            turret["random_release_next_at"] = None
            return

        physical_fireable = get_physical_fireable_shots(turret)

        if physical_fireable <= 0:
            turret["random_release_next_at"] = None
            turret["last_fire_result"] = "Queued shots remain, but reload is needed before random release can continue."
            log_event(
                turret,
                "reload_needed",
                "Queued shots remain, but reload is needed before random release can continue.",
                "warning",
            )
            return

        requested_burst = get_random_burst_count(turret)

        count_to_fire = min(
            requested_burst,
            turret["queued_shots"],
            physical_fireable,
            turret["max_shots_per_redeem"],
        )

        turret["random_release_next_at"] = None

        if count_to_fire > 0:
            await send_fire_command(
                channel_id=channel_id,
                turret_id=turret_id,
                count=count_to_fire,
                source="queue",
            )

    finally:
        current_task = asyncio.current_task()
        if random_release_tasks.get(key) == current_task:
            del random_release_tasks[key]


def schedule_random_release_if_needed(channel_id: str, turret_id: str):
    key = get_connection_key(channel_id, turret_id)

    if key in random_release_tasks and not random_release_tasks[key].done():
        return

    try:
        turret = get_turret(channel_id, turret_id)
    except KeyError:
        return

    if not turret["random_queue_release"]:
        return

    if turret["operation_mode"] != MODE_LIVE_FIRE:
        return

    if not local_system_ready(turret):
        return

    if turret["is_busy"]:
        return

    if turret["queued_shots"] <= 0:
        return

    if get_physical_fireable_shots(turret) <= 0:
        return

    random_release_tasks[key] = asyncio.create_task(
        random_release_worker(channel_id, turret_id)
    )


async def start_next_queued_batch_if_allowed(channel_id: str, turret_id: str):
    turret = get_turret(channel_id, turret_id)

    if turret["queued_shots"] <= 0:
        return

    if turret["operation_mode"] != MODE_LIVE_FIRE:
        return

    if not local_system_ready(turret):
        return

    if turret["is_busy"]:
        return

    if turret["random_queue_release"]:
        schedule_random_release_if_needed(channel_id, turret_id)
        return

    if not turret["auto_fire_queue"]:
        return

    physical_fireable = get_physical_fireable_shots(turret)

    if physical_fireable <= 0:
        turret["last_fire_result"] = "Queued shots remain, but reload is needed."
        log_event(turret, "reload_needed", "Queued shots remain, but reload is needed.", "warning")
        return

    count_to_fire = min(
        turret["queued_shots"],
        turret["max_shots_per_redeem"],
        physical_fireable,
    )

    if count_to_fire <= 0:
        return

    await send_fire_command(
        channel_id=channel_id,
        turret_id=turret_id,
        count=count_to_fire,
        source="queue",
    )


async def apply_fire_result(channel_id: str, turret_id: str, command_id: str, ok: bool, detail: str):
    turret = get_turret(channel_id, turret_id)
    pending = turret["pending_commands"].pop(command_id, None)
    turret["is_busy"] = False

    if pending is None:
        turret["last_fire_result"] = (
            f"Received result for unknown command {command_id}: "
            f"{'OK' if ok else 'FAILED'} {detail}"
        )
        log_event(
            turret,
            "fire_result_unknown",
            turret["last_fire_result"],
            "warning",
            {"command_id": command_id, "ok": ok, "detail": detail},
        )
        return

    count = int(pending["count"])
    source = pending["source"]

    if ok:
        turret["pending_shots"] = max(0, turret["pending_shots"] - count)
        turret["available_shots"] = max(0, turret["available_shots"] - count)

        turret["last_fire_result"] = (
            f"Command {command_id} result: OK. "
            f"{count} shot(s) confirmed accepted by Streamer.bot. {detail}"
        )

        log_event(
            turret,
            "fire_success",
            f"Fire command OK. {count} shot(s) deducted.",
            "success",
            {"command_id": command_id, "count": count, "source": source, "detail": detail},
        )
        save_state_for_turret(turret)

        await start_next_queued_batch_if_allowed(channel_id, turret_id)

    else:
        restore_failed_fire_reservation(turret, count, source)

        log_event(
            turret,
            "fire_failed",
            f"Fire command failed. No shots deducted. {detail}",
            "error",
            {"command_id": command_id, "count": count, "source": source, "detail": detail},
        )

        auto_disable_turret(
            turret,
            (
                f"Command {command_id} result: FAILED. "
                f"No shots were deducted. Foam Cannon auto-disabled. {detail}"
            ),
        )


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Kaizen Foam Cannon EBS",
        "panel": "/panel.html",
        "config": "/config.html",
        "status_endpoint": "/api/status",
        "events_endpoint": "/api/events",
        "control_websocket": "/ws/control",
        "jwt_enforcement": bool(TWITCH_EXTENSION_SECRET),
        "pairing_token_configured": bool(FOAM_CONTROL_TOKEN),
    }


@app.get("/panel.html")
def serve_panel():
    return FileResponse(STATIC_DIR / "panel.html")


@app.get("/config.html")
def serve_config():
    return FileResponse(STATIC_DIR / "config.html")


@app.get("/api/events")
def events(
    channel_id: str = DEFAULT_CHANNEL_ID,
    turret_id: str = DEFAULT_TURRET_ID,
    limit: int = 25,
):
    try:
        turret = get_turret(channel_id, turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    safe_limit = max(1, min(limit, MAX_EVENT_LOG))

    return {
        "ok": True,
        "events": get_recent_events(turret, safe_limit),
    }


@app.get("/api/status")
async def status(
    channel_id: str = DEFAULT_CHANNEL_ID,
    turret_id: str = DEFAULT_TURRET_ID,
):
    try:
        turret = get_turret(channel_id, turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    turret["enabled"] = is_enabled(turret)

    await refresh_stream_status_for_turret(turret)

    return {
        "ok": True,
        "channel_id": channel_id,
        "turret_id": turret_id,
        "display_name": turret["display_name"],
        "gun_type": turret["gun_type"],
        "magazine_capacity": turret["magazine_capacity"],
        "available_shots": turret["available_shots"],
        "queued_shots": turret["queued_shots"],
        "pending_shots": turret["pending_shots"],
        "unreserved_shots": get_unreserved_shots(turret),
        "queue_capacity_remaining": get_queue_capacity_remaining(turret),
        "enabled": turret["enabled"],
        "operation_mode": turret["operation_mode"],
        "auto_fire_queue": turret["auto_fire_queue"],
        "allow_overqueue": turret["allow_overqueue"],
        "max_queue_size": turret["max_queue_size"],
        "random_queue_release": turret["random_queue_release"],
        "random_min_seconds": turret["random_min_seconds"],
        "random_max_seconds": turret["random_max_seconds"],
        "random_burst_enabled": turret["random_burst_enabled"],
        "random_burst_min": turret["random_burst_min"],
        "random_burst_max": turret["random_burst_max"],
        "random_fixed_batch_size": turret["random_fixed_batch_size"],
        "random_release_next_at": turret["random_release_next_at"],
        "is_busy": turret["is_busy"],
        "cooldown_seconds": turret["cooldown_seconds"],
        "max_shots_per_redeem": turret["max_shots_per_redeem"],
        "streamerbot_action": turret["streamerbot_action"],
        "payment_mode": turret["payment_mode"],
        "bits_products": turret["bits_products"],
        "bits_mode_active": turret["payment_mode"] == PAYMENT_BITS,

        # Stream live/offline gate
        "stream_gate_enabled": turret.get("stream_gate_enabled", False),
        "twitch_login": turret.get("twitch_login"),
        "twitch_user_id": turret.get("twitch_user_id"),
        "stream_status": turret.get("stream_status", "unknown"),
        "stream_is_live": turret.get("stream_is_live"),
        "stream_status_checked_at": turret.get("stream_status_checked_at"),
        "stream_status_detail": turret.get(
            "stream_status_detail",
            "Stream status has not been checked yet."
        ),

        "jwt_enforcement": bool(TWITCH_EXTENSION_SECRET),
        "pairing_token_configured": bool(turret.get("control_token")),
        "setup_checks": format_setup_checks(turret),
        "recent_events": get_recent_events(turret, 15),
        "connection_status": turret["connection_status"],
        "streamerbot_status": turret["streamerbot_status"],
        "streamerbot_detail": turret["streamerbot_detail"],
        "last_seen": turret["last_seen"],
        "last_fire_result": turret["last_fire_result"],
        "can_fire_now": get_can_fire_now(turret),
        "can_queue": get_can_queue(turret),
        "viewer_action_label": get_viewer_action_label(turret),
        "viewer_message": get_viewer_message(turret),
    }


@app.post("/api/request-shots")
async def request_shots(request: ShotRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if request.count <= 0:
        return {"ok": False, "error": "Shot count must be greater than zero."}

    if request.count > turret["max_shots_per_redeem"]:
        return {
            "ok": False,
            "error": f"Maximum shots per request is {turret['max_shots_per_redeem']}.",
        }

    if turret["connection_status"] != "online":
        auto_disable_turret(
            turret,
            "Auto-disabled because the local control client is offline.",
        )
        return {"ok": False, "error": "Local control client is offline."}

    if turret["streamerbot_status"] != "online":
        auto_disable_turret(
            turret,
            f"Auto-disabled because Streamer.bot is offline. {turret['streamerbot_detail']}",
        )
        return {"ok": False, "error": f"Streamer.bot is offline. {turret['streamerbot_detail']}"}

    if turret["operation_mode"] == MODE_DISABLED:
        return {"ok": False, "error": "Foam Cannon is disabled."}

    gate_ok, gate_message = await stream_gate_allows_fire(turret)

    if not gate_ok:
        log_event(
            turret,
            "stream_gate_blocked",
            gate_message,
            "warning",
        )
        return {"ok": False, "error": gate_message}


    if (
        turret["operation_mode"] == MODE_LIVE_FIRE
        and not turret["is_busy"]
        and request.count <= get_physical_fireable_shots(turret)
    ):
        result = await send_fire_command(
            channel_id=request.channel_id,
            turret_id=request.turret_id,
            count=request.count,
            source="live_fire",
        )
        result["mode"] = MODE_LIVE_FIRE
        return result

    queue_result = add_to_queue(turret, request.count, "Viewer request")

    if queue_result["ok"]:
        if turret["operation_mode"] == MODE_LIVE_FIRE:
            queue_result["message"] = f"Cannon is firing or not ready. Queued {request.count} shot(s)."
            schedule_random_release_if_needed(request.channel_id, request.turret_id)

        elif turret["operation_mode"] == MODE_QUEUE_ONLY:
            queue_result["message"] = f"Queued {request.count} shot(s) for later."

        queue_result["mode"] = turret["operation_mode"]

    return queue_result


@app.post("/api/request-shots-with-transaction")
async def request_shots_with_transaction(
    request: BitsTransactionRequest,
    authorization: str | None = Header(default=None),
):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if turret["payment_mode"] != PAYMENT_BITS:
        log_event(
            turret,
            "bits_rejected",
            "Bits transaction rejected because payment mode is not Bits.",
            "warning",
            {"transaction_id": request.transaction_id, "sku": request.sku},
        )
        return {
            "ok": False,
            "error": "Bits transaction received, but payment mode is not set to bits.",
        }

    jwt_result = decode_twitch_extension_jwt(authorization)

    if not jwt_result["ok"]:
        log_event(
            turret,
            "bits_jwt_rejected",
            jwt_result["error"],
            "error",
            {"transaction_id": request.transaction_id, "sku": request.sku},
        )
        return {
            "ok": False,
            "error": jwt_result["error"],
        }

    if not request.transaction_id:
        return {"ok": False, "error": "Missing transaction ID."}

    if request.transaction_id in turret["processed_transactions"] or transaction_exists(request.transaction_id):
        log_event(
            turret,
            "bits_duplicate",
            f"Duplicate transaction ignored: {request.transaction_id}.",
            "warning",
            {"transaction_id": request.transaction_id, "sku": request.sku},
        )
        return {
            "ok": False,
            "error": "Duplicate transaction ignored.",
            "transaction_id": request.transaction_id,
        }

    product = turret["bits_products"].get(request.sku)

    if product is None:
        return {
            "ok": False,
            "error": f"Unknown Bits product SKU: {request.sku}",
        }

    expected_bits = int(product["bits"])

    if request.product_cost is not None and int(request.product_cost) != expected_bits:
        return {
            "ok": False,
            "error": (
                f"Bits amount mismatch. Expected {expected_bits}, "
                f"received {request.product_cost}."
            ),
        }

    claims = jwt_result.get("claims") or {}

    jwt_channel_id = claims.get("channel_id") or claims.get("channelId")
    jwt_user_id = claims.get("opaque_user_id") or claims.get("user_id")

    shot_count = int(product["shots"])

    turret["processed_transactions"][request.transaction_id] = {
        "sku": request.sku,
        "shots": shot_count,
        "bits": expected_bits,
        "user_id": request.user_id or jwt_user_id,
        "user_name": request.user_name,
        "jwt_channel_id": jwt_channel_id,
        "jwt_enforced": jwt_result.get("enforced", False),
        "received_at": time.time(),
        "status": "received",
    }

    log_event(
        turret,
        "bits_received",
        f"Bits transaction received: {expected_bits} Bits for {shot_count} shot(s).",
        "info",
        {
            "transaction_id": request.transaction_id,
            "sku": request.sku,
            "bits": expected_bits,
            "shots": shot_count,
            "jwt_enforced": jwt_result.get("enforced", False),
        },
    )

    persist_transaction(
        transaction_id=request.transaction_id,
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        sku=request.sku,
        bits=expected_bits,
        shots=shot_count,
        status="received",
        user_id=request.user_id or jwt_user_id,
        user_name=request.user_name,
        jwt_channel_id=jwt_channel_id,
        jwt_enforced=jwt_result.get("enforced", False),
        received_at=turret["processed_transactions"][request.transaction_id]["received_at"],
    )

    result = await request_shots(ShotRequest(
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        count=shot_count,
    ))

    turret["processed_transactions"][request.transaction_id]["result"] = result
    turret["processed_transactions"][request.transaction_id]["status"] = (
        "accepted" if result.get("ok") else "failed"
    )

    result["transaction_id"] = request.transaction_id
    result["sku"] = request.sku
    result["bits"] = expected_bits
    result["jwt_enforced"] = jwt_result.get("enforced", False)

    log_event(
        turret,
        "bits_result",
        f"Bits transaction {request.transaction_id} result: {'accepted' if result.get('ok') else 'failed'}.",
        "success" if result.get("ok") else "error",
        {
            "transaction_id": request.transaction_id,
            "sku": request.sku,
            "result": result,
        },
    )

    persist_transaction(
        transaction_id=request.transaction_id,
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        sku=request.sku,
        bits=expected_bits,
        shots=shot_count,
        status="accepted" if result.get("ok") else "failed",
        user_id=request.user_id or jwt_user_id,
        user_name=request.user_name,
        jwt_channel_id=jwt_channel_id,
        jwt_enforced=jwt_result.get("enforced", False),
        received_at=turret["processed_transactions"][request.transaction_id]["received_at"],
        result=result,
    )

    return result


@app.post("/api/fire")
async def fire(request: ShotRequest):
    return await request_shots(request)


@app.post("/api/queue/add")
async def queue_add(request: ShotRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    result = add_to_queue(turret, request.count, "Streamer manual add")

    if result["ok"]:
        schedule_random_release_if_needed(request.channel_id, request.turret_id)

    return result


@app.post("/api/reload")
def reload(request: ReloadRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if request.capacity <= 0:
        return {"ok": False, "error": "Capacity must be greater than zero."}

    if turret["is_busy"]:
        return {
            "ok": False,
            "error": "Cannot reload while turret is processing a fire command.",
        }

    if not turret["allow_overqueue"]:
        if request.capacity < turret["queued_shots"] + turret["pending_shots"]:
            return {
                "ok": False,
                "error": (
                    f"Reload capacity cannot be less than queued + pending shots. "
                    f"Queued: {turret['queued_shots']}, pending: {turret['pending_shots']}, "
                    f"requested capacity: {request.capacity}."
                ),
            }

    turret["magazine_capacity"] = request.capacity
    turret["available_shots"] = request.capacity
    turret["last_fire_result"] = (
        f"Reloaded to {request.capacity}. Queue preserved at {turret['queued_shots']} shot(s)."
    )

    log_event(
        turret,
        "reload",
        f"Reloaded to {request.capacity}. Queue preserved at {turret['queued_shots']} shot(s).",
        "success",
        {"capacity": request.capacity, "queued_shots": turret["queued_shots"]},
    )
    save_state_for_turret(turret)

    return {
        "ok": True,
        "message": (
            f"Reloaded to {request.capacity}. Queue preserved at {turret['queued_shots']} shot(s)."
        ),
        "available_shots": turret["available_shots"],
        "queued_shots": turret["queued_shots"],
        "pending_shots": turret["pending_shots"],
        "unreserved_shots": get_unreserved_shots(turret),
        "queue_capacity_remaining": get_queue_capacity_remaining(turret),
        "magazine_capacity": turret["magazine_capacity"],
    }


@app.post("/api/mode")
async def set_mode(request: ModeRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    valid_modes = {MODE_LIVE_FIRE, MODE_QUEUE_ONLY, MODE_DISABLED}

    if request.operation_mode not in valid_modes:
        return {
            "ok": False,
            "error": f"Invalid mode. Valid modes: {', '.join(sorted(valid_modes))}",
        }

    if request.operation_mode in {MODE_LIVE_FIRE, MODE_QUEUE_ONLY} and not local_system_ready(turret):
        return {
            "ok": False,
            "error": "Cannot arm Foam Cannon while local control or Streamer.bot is offline.",
        }

    previous_mode = turret["operation_mode"]

    turret["operation_mode"] = request.operation_mode
    turret["enabled"] = is_enabled(turret)
    turret["last_fire_result"] = f"Mode changed to {request.operation_mode}."

    log_event(
        turret,
        "mode_change",
        f"Mode changed from {previous_mode} to {request.operation_mode}.",
        "info",
        {"previous_mode": previous_mode, "new_mode": request.operation_mode},
    )
    save_state_for_turret(turret)

    if request.operation_mode == MODE_LIVE_FIRE:
        await start_next_queued_batch_if_allowed(request.channel_id, request.turret_id)

    return {
        "ok": True,
        "message": f"Mode changed to {request.operation_mode}.",
        "operation_mode": turret["operation_mode"],
    }


@app.post("/api/settings/payment")
def set_payment_settings(request: PaymentSettingsRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    valid_modes = {PAYMENT_FREE_TEST, PAYMENT_BITS}

    if request.payment_mode not in valid_modes:
        return {
            "ok": False,
            "error": "Invalid payment mode. Use free_test or bits.",
        }

    previous_mode = turret["payment_mode"]

    turret["payment_mode"] = request.payment_mode
    turret["last_fire_result"] = f"Payment mode set to {request.payment_mode}."

    log_event(
        turret,
        "payment_mode_change",
        f"Payment mode changed from {previous_mode} to {request.payment_mode}.",
        "info",
        {"previous_mode": previous_mode, "new_mode": request.payment_mode},
    )
    save_state_for_turret(turret)

    return {
        "ok": True,
        "message": turret["last_fire_result"],
        "payment_mode": turret["payment_mode"],
    }


@app.post("/api/settings/queue")
async def set_queue_settings(request: QueueSettingsRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if request.max_queue_size < 0:
        return {"ok": False, "error": "Max queue size cannot be negative."}

    if request.max_queue_size < turret["queued_shots"]:
        return {
            "ok": False,
            "error": f"Max queue size cannot be less than current queued shots: {turret['queued_shots']}.",
        }

    if request.random_min_seconds < 0 or request.random_max_seconds < 0:
        return {"ok": False, "error": "Random delay cannot be negative."}

    if request.random_max_seconds < request.random_min_seconds:
        return {"ok": False, "error": "Random max delay must be greater than or equal to min delay."}

    if request.random_burst_min <= 0 or request.random_burst_max <= 0:
        return {"ok": False, "error": "Random burst values must be greater than zero."}

    if request.random_burst_max < request.random_burst_min:
        return {"ok": False, "error": "Random burst max must be greater than or equal to min."}

    turret["allow_overqueue"] = request.allow_overqueue
    turret["max_queue_size"] = request.max_queue_size
    turret["random_queue_release"] = request.random_queue_release
    turret["random_min_seconds"] = request.random_min_seconds
    turret["random_max_seconds"] = request.random_max_seconds
    turret["random_burst_enabled"] = request.random_burst_enabled
    turret["random_burst_min"] = request.random_burst_min
    turret["random_burst_max"] = request.random_burst_max
    turret["random_fixed_batch_size"] = max(1, request.random_fixed_batch_size)

    turret["last_fire_result"] = "Queue settings updated."

    log_event(
        turret,
        "queue_settings",
        "Queue settings updated.",
        "info",
        {
            "allow_overqueue": request.allow_overqueue,
            "max_queue_size": request.max_queue_size,
            "random_queue_release": request.random_queue_release,
            "random_min_seconds": request.random_min_seconds,
            "random_max_seconds": request.random_max_seconds,
            "random_burst_enabled": request.random_burst_enabled,
            "random_burst_min": request.random_burst_min,
            "random_burst_max": request.random_burst_max,
            "random_fixed_batch_size": request.random_fixed_batch_size,
        },
    )
    save_state_for_turret(turret)

    schedule_random_release_if_needed(request.channel_id, request.turret_id)

    return {
        "ok": True,
        "message": "Queue settings updated.",
        "allow_overqueue": turret["allow_overqueue"],
        "max_queue_size": turret["max_queue_size"],
        "random_queue_release": turret["random_queue_release"],
        "random_burst_enabled": turret["random_burst_enabled"],
    }


@app.post("/api/auto-fire-queue")
async def set_auto_fire_queue(request: AutoFireQueueRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    turret["auto_fire_queue"] = request.auto_fire_queue
    turret["last_fire_result"] = (
        f"Auto-fire queue set to {'ON' if turret['auto_fire_queue'] else 'OFF'}."
    )

    log_event(turret, "auto_fire_queue", turret["last_fire_result"], "info")
    save_state_for_turret(turret)

    await start_next_queued_batch_if_allowed(request.channel_id, request.turret_id)

    return {
        "ok": True,
        "message": turret["last_fire_result"],
        "auto_fire_queue": turret["auto_fire_queue"],
    }


@app.post("/api/enable")
async def enable(request: TurretActionRequest):
    return await set_mode(ModeRequest(
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        operation_mode=MODE_LIVE_FIRE,
    ))


@app.post("/api/disable")
async def disable(request: TurretActionRequest):
    return await set_mode(ModeRequest(
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        operation_mode=MODE_DISABLED,
    ))


@app.post("/api/open-queue")
async def open_queue(request: TurretActionRequest):
    return await set_mode(ModeRequest(
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        operation_mode=MODE_QUEUE_ONLY,
    ))


@app.post("/api/set-empty")
def set_empty(request: TurretActionRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if turret["is_busy"]:
        return {
            "ok": False,
            "error": "Cannot set empty while turret is processing a fire command.",
        }

    turret["available_shots"] = 0
    turret["queued_shots"] = 0
    turret["pending_shots"] = 0
    turret["pending_commands"] = {}
    turret["random_release_next_at"] = None
    turret["last_fire_result"] = "Shot count set to empty. Queue cleared."

    log_event(turret, "set_empty", "Shot count set to empty. Queue cleared.", "warning")
    save_state_for_turret(turret)

    return {
        "ok": True,
        "message": "Shot count set to empty. Queue cleared.",
        "available_shots": turret["available_shots"],
        "queued_shots": turret["queued_shots"],
        "pending_shots": turret["pending_shots"],
    }


@app.post("/api/queue/clear")
def clear_queue(request: TurretActionRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if turret["is_busy"]:
        return {
            "ok": False,
            "error": "Cannot clear queue while turret is firing.",
        }

    cleared_count = turret["queued_shots"]

    turret["queued_shots"] = 0
    turret["random_release_next_at"] = None
    turret["last_fire_result"] = "Queue cleared."

    log_event(turret, "queue_clear", f"Queue cleared. Removed {cleared_count} shot(s).", "warning")
    save_state_for_turret(turret)

    return {
        "ok": True,
        "message": "Queue cleared.",
        "queued_shots": turret["queued_shots"],
        "pending_shots": turret["pending_shots"],
        "unreserved_shots": get_unreserved_shots(turret),
        "queue_capacity_remaining": get_queue_capacity_remaining(turret),
    }


@app.post("/api/queue/fire")
async def fire_queue(request: FireQueueRequest):
    try:
        turret = get_turret(request.channel_id, request.turret_id)
    except KeyError as error:
        return {"ok": False, "error": str(error)}

    if not local_system_ready(turret):
        auto_disable_turret(
            turret,
            "Auto-disabled because local control or Streamer.bot is offline.",
        )
        return {"ok": False, "error": "Local control or Streamer.bot is offline."}

    if turret["operation_mode"] == MODE_DISABLED:
        return {"ok": False, "error": "Foam Cannon is disabled."}

    if turret["is_busy"]:
        return {
            "ok": False,
            "error": "Turret is currently processing another fire command.",
        }

    if turret["queued_shots"] <= 0:
        return {"ok": False, "error": "Queue is empty."}

    physical_fireable = get_physical_fireable_shots(turret)

    if physical_fireable <= 0:
        log_event(turret, "reload_needed", "No loaded shots available. Reload needed.", "warning")
        return {"ok": False, "error": "No loaded shots available. Reload needed."}

    count_to_fire = turret["queued_shots"] if request.count is None else int(request.count)

    if count_to_fire <= 0:
        return {"ok": False, "error": "Queue fire count must be greater than zero."}

    count_to_fire = min(
        count_to_fire,
        turret["queued_shots"],
        turret["max_shots_per_redeem"],
        physical_fireable,
    )

    result = await send_fire_command(
        channel_id=request.channel_id,
        turret_id=request.turret_id,
        count=count_to_fire,
        source="queue",
    )

    return result


@app.websocket("/ws/control")
async def control_socket(websocket: WebSocket):
    await websocket.accept()

    channel_id = None
    turret_id = None
    connection_key = None

    try:
        hello = await websocket.receive_json()

        if hello.get("type") != "hello":
            await websocket.send_json({
                "type": "error",
                "error": "Expected hello message.",
            })
            await websocket.close()
            return

        channel_id = hello.get("channel_id", DEFAULT_CHANNEL_ID)
        turret_id = hello.get("turret_id", DEFAULT_TURRET_ID)
        provided_control_token = hello.get("control_token")

        try:
            turret = get_turret(channel_id, turret_id)
        except KeyError as error:
            await websocket.send_json({
                "type": "error",
                "error": str(error),
            })
            await websocket.close()
            return

        token_ok, token_message = verify_control_token(turret, provided_control_token)

        if not token_ok:
            log_event(
                turret,
                "pairing_rejected",
                f"Local-control pairing rejected: {token_message}",
                "error",
                {"channel_id": channel_id, "turret_id": turret_id},
            )

            await websocket.send_json({
                "type": "error",
                "error": token_message,
            })
            await websocket.close()
            return

        log_event(
            turret,
            "pairing_accepted",
            "Local-control pairing token accepted.",
            "success",
            {"channel_id": channel_id, "turret_id": turret_id},
        )

        connection_key = get_connection_key(channel_id, turret_id)
        control_connections[connection_key] = websocket

        turret["connection_status"] = "online"
        turret["streamerbot_status"] = hello.get("streamerbot_status", "unknown")
        turret["streamerbot_detail"] = hello.get("streamerbot_detail", "No Streamer.bot detail supplied.")
        turret["last_seen"] = time.time()
        turret["last_fire_result"] = "Local control client connected."

        log_event(
            turret,
            "local_control_connected",
            "Local control client connected.",
            "success",
            {
                "channel_id": channel_id,
                "turret_id": turret_id,
                "streamerbot_status": turret["streamerbot_status"],
                "streamerbot_detail": turret["streamerbot_detail"],
            },
        )

        if turret["streamerbot_status"] != "online":
            auto_disable_turret(
                turret,
                f"Auto-disabled because Streamer.bot is offline. {turret['streamerbot_detail']}",
            )

        await websocket.send_json({
            "type": "hello_ack",
            "message": "Local control client connected.",
            "channel_id": channel_id,
            "turret_id": turret_id,
        })

        while True:
            message = await websocket.receive_json()
            turret["last_seen"] = time.time()

            if message.get("streamerbot_status"):
                previous_streamerbot_status = turret["streamerbot_status"]

                turret["streamerbot_status"] = message.get("streamerbot_status")
                turret["streamerbot_detail"] = message.get("streamerbot_detail", "")

                if turret["streamerbot_status"] != "online":
                    log_event(
                        turret,
                        "streamerbot_offline",
                        f"Streamer.bot offline: {turret['streamerbot_detail']}",
                        "warning",
                    )
                    auto_disable_turret(
                        turret,
                        f"Auto-disabled because Streamer.bot is offline. {turret['streamerbot_detail']}",
                    )

                elif previous_streamerbot_status != "online":
                    turret["last_fire_result"] = (
                        "Streamer.bot is back online. Foam Cannon remains disabled until manually re-armed."
                    )
                    log_event(
                        turret,
                        "streamerbot_online",
                        "Streamer.bot is back online. Cannon remains disabled until manually re-armed.",
                        "success",
                    )

            if message.get("type") == "heartbeat":
                await websocket.send_json({
                    "type": "heartbeat_ack",
                    "time": time.time(),
                })

            elif message.get("type") == "status":
                await websocket.send_json({
                    "type": "status_ack",
                    "time": time.time(),
                })

            elif message.get("type") == "fire_result":
                ok = message.get("ok", False)
                command_id = message.get("command_id")
                detail = message.get("detail", "")

                await apply_fire_result(
                    channel_id=channel_id,
                    turret_id=turret_id,
                    command_id=command_id,
                    ok=ok,
                    detail=detail,
                )

            else:
                await websocket.send_json({
                    "type": "warning",
                    "message": f"Unknown message type: {message.get('type')}",
                })

    except WebSocketDisconnect:
        pass

    finally:
        if connection_key and control_connections.get(connection_key) == websocket:
            del control_connections[connection_key]

        if channel_id and turret_id:
            try:
                turret = get_turret(channel_id, turret_id)
                turret["connection_status"] = "offline"
                turret["streamerbot_status"] = "unknown"
                turret["streamerbot_detail"] = "Local control client disconnected."
                auto_disable_turret(
                    turret,
                    "Auto-disabled because local control client disconnected.",
                )
            except KeyError:
                pass