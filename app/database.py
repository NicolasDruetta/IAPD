import os
import psycopg
import threading

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from typing import Any

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False
MARKET_ANSWERS_TABLE = "market_answers"
MARKET_METRICS_TABLE = "market_metrics"
POSTGRES_DB = os.getenv("POSTGRES_DB", "apache_big_data_lab")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "apache_big_data_lab")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")

def ensure_tables(connection: psycopg.Connection) -> None:
    """Creo las tablas analíticas y verifico sus columnas compatibles."""

    global _SCHEMA_READY
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {MARKET_METRICS_TABLE} (
                    run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    feed TEXT NOT NULL,
                    trade_count BIGINT NOT NULL,
                    return_count BIGINT NOT NULL,
                    volatility DOUBLE PRECISION,
                    spread_count BIGINT NOT NULL,
                    spread_mean DOUBLE PRECISION,
                    total_volume DOUBLE PRECISION,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, symbol)
                )
                """,
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {MARKET_ANSWERS_TABLE} (
                    run_id TEXT NOT NULL,
                    question_id TEXT NOT NULL,
                    feed TEXT NOT NULL,
                    correlation DOUBLE PRECISION,
                    symbol_count BIGINT NOT NULL,
                    top_volume JSONB,
                    activity_rows JSONB,
                    activity_interval INTEGER,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, question_id)
                )
                """,
            )
            cursor.execute(
                f"ALTER TABLE {MARKET_ANSWERS_TABLE} ADD COLUMN IF NOT EXISTS top_volume JSONB"
            )
            cursor.execute(
                f"ALTER TABLE {MARKET_ANSWERS_TABLE} ADD COLUMN IF NOT EXISTS activity_rows JSONB"
            )
            cursor.execute(
                f"ALTER TABLE {MARKET_ANSWERS_TABLE} ADD COLUMN IF NOT EXISTS activity_interval INTEGER"
            )
            cursor.execute(
                f"ALTER TABLE {MARKET_ANSWERS_TABLE} DROP COLUMN IF EXISTS conclusion"
            )
        connection.commit()
        _SCHEMA_READY = True

def get_market_analysis(run_id: str | None) -> dict[str, Any]:
    """Consulto PostgreSQL y devuelvo datos para preguntas y gráficos."""

    if not run_id:
        return {
            "activity_interval": 1,
            "activity_rows": [],
            "answers": [],
            "metrics": [],
            "top_volume": [],
        }
    try:
        with get_postgresql_connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    "SELECT symbol, trade_count, return_count, volatility, spread_count, spread_mean, total_volume FROM market_metrics WHERE run_id = %s",
                    (run_id,),
                )
                metrics = cursor.fetchall()
                cursor.execute(
                    "SELECT question_id, feed, correlation, symbol_count, top_volume, activity_rows, activity_interval FROM market_answers WHERE run_id = %s",
                    (run_id,),
                )
                answers = cursor.fetchall()
                chart_answer = next(
                    (
                        answer
                        for answer in answers
                        if answer["question_id"] == "volatility"
                    ),
                    {},
                )
        return {
            "activity_interval": chart_answer.get("activity_interval", 1),
            "activity_rows": chart_answer.get("activity_rows") or [],
            "answers": answers,
            "metrics": metrics,
            "top_volume": chart_answer.get("top_volume") or [],
        }
    except (OSError, psycopg.Error):
        return {
            "activity_interval": 1,
            "activity_rows": [],
            "answers": [],
            "metrics": [],
            "top_volume": [],
        }

def get_postgresql_connection() -> psycopg.Connection:
    """Abro una conexión PostgreSQL con la configuración del contenedor."""

    return psycopg.connect(
        dbname=POSTGRES_DB,
        host=POSTGRES_HOST,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
    )

def persist_analysis(
    connection: psycopg.Connection,
    run_id: str,
    metrics: list[dict[str, Any]],
    answers: list[dict[str, Any]],
    top_volume: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    activity_interval: int,
) -> dict[str, int]:
    """Actualizo el RUN completo mediante UPSERT idempotente."""

    inserted_metrics = 0
    updated_metrics = 0
    inserted_answers = 0
    updated_answers = 0
    with connection.cursor() as cursor:
        for metric in metrics:
            cursor.execute(
                f"""
                INSERT INTO {MARKET_METRICS_TABLE} (
                    run_id, symbol, feed, trade_count, return_count,
                    volatility, spread_count, spread_mean, total_volume
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, symbol) DO UPDATE SET
                    feed = EXCLUDED.feed,
                    trade_count = EXCLUDED.trade_count,
                    return_count = EXCLUDED.return_count,
                    volatility = EXCLUDED.volatility,
                    spread_count = EXCLUDED.spread_count,
                    spread_mean = EXCLUDED.spread_mean,
                    total_volume = EXCLUDED.total_volume,
                    updated_at = NOW()
                RETURNING (xmax = 0)
                """,
                (
                    run_id,
                    metric["symbol"],
                    metric["feed"],
                    metric["trade_count"] or 0,
                    metric["return_count"] or 0,
                    metric["volatility"],
                    metric["spread_count"] or 0,
                    metric["spread_mean"],
                    metric["total_volume"] or 0.0,
                ),
            )
            if cursor.fetchone()[0]:
                inserted_metrics += 1
            else:
                updated_metrics += 1
        for answer in answers:
            cursor.execute(
                f"""
                INSERT INTO {MARKET_ANSWERS_TABLE} (
                    run_id, question_id, feed, correlation, symbol_count,
                    top_volume, activity_rows, activity_interval
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, question_id) DO UPDATE SET
                    feed = EXCLUDED.feed,
                    correlation = EXCLUDED.correlation,
                    symbol_count = EXCLUDED.symbol_count,
                    top_volume = EXCLUDED.top_volume,
                    activity_rows = EXCLUDED.activity_rows,
                    activity_interval = EXCLUDED.activity_interval,
                    updated_at = NOW()
                RETURNING (xmax = 0)
                """,
                (
                    run_id,
                    answer["question_id"],
                    answer["feed"],
                    answer["correlation"],
                    answer["symbol_count"],
                    Jsonb(top_volume) if answer["question_id"] == "volatility" else None,
                    Jsonb(activity_rows) if answer["question_id"] == "volatility" else None,
                    activity_interval if answer["question_id"] == "volatility" else None,
                ),
            )
            if cursor.fetchone()[0]:
                inserted_answers += 1
            else:
                updated_answers += 1
    connection.commit()
    return {
        "inserted": inserted_metrics + inserted_answers,
        "updated": updated_metrics + updated_answers,
    }
