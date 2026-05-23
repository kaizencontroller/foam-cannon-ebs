import json
import os
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def persistence_enabled() -> bool:
    return bool(DATABASE_URL)


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")

    return psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,
    )


def init_db():
    if not persistence_enabled():
        print("DATABASE_URL not found. Running in memory-only mode.")
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS turret_state (
                    channel_id TEXT NOT NULL,
                    turret_id TEXT NOT NULL,
                    state_json JSONB NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (channel_id, turret_id)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS event_log (
                    id BIGSERIAL PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    turret_id TEXT NOT NULL,
                    event_timestamp DOUBLE PRECISION NOT NULL,
                    event_type TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    extra_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_log_turret_time
                ON event_log (channel_id, turret_id, event_timestamp DESC);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    turret_id TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    bits INTEGER NOT NULL,
                    shots INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    user_id TEXT,
                    user_name TEXT,
                    jwt_channel_id TEXT,
                    jwt_enforced BOOLEAN NOT NULL DEFAULT FALSE,
                    received_at DOUBLE PRECISION NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL,
                    result_json JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transactions_turret_time
                ON transactions (channel_id, turret_id, received_at DESC);
                """
            )

    print("Database persistence enabled.")


def load_turret_state(channel_id: str, turret_id: str) -> dict[str, Any] | None:
    if not persistence_enabled():
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT state_json
                FROM turret_state
                WHERE channel_id = %s AND turret_id = %s;
                """,
                (channel_id, turret_id),
            )
            row = cur.fetchone()

    if not row:
        return None

    return dict(row["state_json"])


def save_turret_state(channel_id: str, turret_id: str, state: dict[str, Any]):
    if not persistence_enabled():
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO turret_state (
                    channel_id,
                    turret_id,
                    state_json,
                    updated_at
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (channel_id, turret_id)
                DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    updated_at = EXCLUDED.updated_at;
                """,
                (
                    channel_id,
                    turret_id,
                    json.dumps(state),
                    time.time(),
                ),
            )


def persist_event(
    channel_id: str,
    turret_id: str,
    event_type: str,
    level: str,
    message: str,
    extra: dict[str, Any] | None = None,
    event_timestamp: float | None = None,
):
    if not persistence_enabled():
        return

    if extra is None:
        extra = {}

    if event_timestamp is None:
        event_timestamp = time.time()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_log (
                    channel_id,
                    turret_id,
                    event_timestamp,
                    event_type,
                    level,
                    message,
                    extra_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    channel_id,
                    turret_id,
                    event_timestamp,
                    event_type,
                    level,
                    message,
                    json.dumps(extra),
                ),
            )


def load_recent_events(
    channel_id: str,
    turret_id: str,
    limit: int = 25,
) -> list[dict[str, Any]]:
    if not persistence_enabled():
        return []

    safe_limit = max(1, min(int(limit), 100))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    event_timestamp AS timestamp,
                    event_type,
                    level,
                    message,
                    extra_json AS extra
                FROM event_log
                WHERE channel_id = %s AND turret_id = %s
                ORDER BY event_timestamp DESC
                LIMIT %s;
                """,
                (channel_id, turret_id, safe_limit),
            )

            rows = cur.fetchall()

    return [dict(row) for row in rows]


def persist_transaction(
    transaction_id: str,
    channel_id: str,
    turret_id: str,
    sku: str,
    bits: int,
    shots: int,
    status: str,
    user_id: str | None = None,
    user_name: str | None = None,
    jwt_channel_id: str | None = None,
    jwt_enforced: bool = False,
    received_at: float | None = None,
    result: dict[str, Any] | None = None,
):
    if not persistence_enabled():
        return

    if received_at is None:
        received_at = time.time()

    if result is None:
        result = {}

    now = time.time()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (
                    transaction_id,
                    channel_id,
                    turret_id,
                    sku,
                    bits,
                    shots,
                    status,
                    user_id,
                    user_name,
                    jwt_channel_id,
                    jwt_enforced,
                    received_at,
                    updated_at,
                    result_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at,
                    result_json = EXCLUDED.result_json;
                """,
                (
                    transaction_id,
                    channel_id,
                    turret_id,
                    sku,
                    bits,
                    shots,
                    status,
                    user_id,
                    user_name,
                    jwt_channel_id,
                    jwt_enforced,
                    received_at,
                    now,
                    json.dumps(result),
                ),
            )


def transaction_exists(transaction_id: str) -> bool:
    if not persistence_enabled():
        return False

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM transactions
                WHERE transaction_id = %s
                LIMIT 1;
                """,
                (transaction_id,),
            )
            row = cur.fetchone()

    return row is not None