import functions
import math

import pandas as pd
import psycopg

from psycopg.types.json import Jsonb
from typing import Any

FEED_NAME = "IEX"
MARKET_ANSWERS_TABLE = "market_answers"
MARKET_METRICS_TABLE = "market_metrics"

def analyze_and_persist(
    run_id: str,
    aggregate_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    feed: str = FEED_NAME,
) -> bool:
    """Calculo el análisis acumulado y guardo solo sus resultados."""

    if not aggregate_rows:
        return False
    try:
        metrics, answers = calculate_analysis(aggregate_rows, feed)
        top_volume, activity, activity_interval = prepare_chart_data(
            aggregate_rows,
            activity_rows,
        )
        with functions.get_postgresql_connection() as connection:
            ensure_tables(connection)
            persist_analysis(
                connection,
                run_id,
                metrics,
                answers,
                top_volume,
                activity,
                activity_interval,
            )
        functions.append_console_log(
            run_id,
            "pandas_postgresql",
            f"AGGREGATES | recibidos={len(aggregate_rows)} | feed={feed}",
        )
        functions.append_console_log(
            run_id,
            "pandas_postgresql",
            f"PANDAS | correlaciones_calculadas={len(answers)} | feed={feed}",
        )
        functions.append_console_log(
            run_id,
            "pandas_postgresql",
            f"POSTGRES | metricas_guardadas={len(metrics)} | respuestas_persistidas={len(answers)}",
        )
        return True
    except (OSError, TypeError, ValueError, psycopg.Error) as error:
        functions.append_console_log(
            run_id,
            "pandas_postgresql",
            f"ERROR | análisis={error}",
        )
        return False

def calculate_analysis(
    aggregate_rows: list[dict[str, Any]],
    feed: str = FEED_NAME,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Calculo métricas por símbolo y las dos respuestas con Pandas."""

    frame = pd.DataFrame(aggregate_rows)
    if frame.empty:
        return [], []
    numeric_columns = ["return_count", "return_sum", "return_sum_squares", "spread_count", "spread_sum", "trade_count", "total_volume"]
    for column in numeric_columns:
        if column not in frame:
            frame[column] = pd.NA
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["volatility"] = frame.apply(_sample_standard_deviation, axis=1)
    frame["spread_mean"] = frame.apply(_mean_spread, axis=1)
    metrics = [
        {
            "feed": feed,
            "return_count": _integer_or_none(row.return_count),
            "spread_count": _integer_or_none(row.spread_count),
            "spread_mean": _float_or_none(row.spread_mean),
            "symbol": str(row.symbol),
            "total_volume": _float_or_none(row.total_volume),
            "trade_count": _integer_or_none(row.trade_count),
            "volatility": _float_or_none(row.volatility),
        }
        for row in frame.itertuples(index=False)
    ]
    answers = [
        _build_answer(
            frame,
            "volatility",
            "Las acciones con más operaciones presentan mayor volatilidad.",
            feed,
        ),
        _build_answer(
            frame,
            "spread_mean",
            "Las acciones más negociadas tienen menor spread entre compra y venta.",
            feed,
        ),
    ]
    return metrics, answers

def ensure_tables(connection: psycopg.Connection) -> None:
    """Creo las dos tablas analíticas si todavía no existen."""

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
                conclusion TEXT NOT NULL,
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

def persist_analysis(
    connection: psycopg.Connection,
    run_id: str,
    metrics: list[dict[str, Any]],
    answers: list[dict[str, Any]],
    top_volume: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    activity_interval: int,
) -> None:
    """Actualizo el RUN completo mediante UPSERT idempotente."""

    with connection.cursor() as cursor:
        cursor.executemany(
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
            """,
            [
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
                )
                for metric in metrics
            ],
        )
        cursor.executemany(
            f"""
            INSERT INTO {MARKET_ANSWERS_TABLE} (
                run_id, question_id, feed, correlation, symbol_count,
                conclusion, top_volume, activity_rows, activity_interval
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, question_id) DO UPDATE SET
                feed = EXCLUDED.feed,
                correlation = EXCLUDED.correlation,
                symbol_count = EXCLUDED.symbol_count,
                conclusion = EXCLUDED.conclusion,
                top_volume = EXCLUDED.top_volume,
                activity_rows = EXCLUDED.activity_rows,
                activity_interval = EXCLUDED.activity_interval,
                updated_at = NOW()
            """,
            [
                (
                    run_id,
                    answer["question_id"],
                    answer["feed"],
                    answer["correlation"],
                    answer["symbol_count"],
                    answer["conclusion"],
                    Jsonb(top_volume) if answer["question_id"] == "volatility" else None,
                    Jsonb(activity_rows) if answer["question_id"] == "volatility" else None,
                    activity_interval if answer["question_id"] == "volatility" else None,
                )
                for answer in answers
            ],
        )
    connection.commit()

def prepare_chart_data(
    aggregate_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Preparo en Pandas los datos completos de los dos gráficos."""

    volume_frame = pd.DataFrame(aggregate_rows)
    if volume_frame.empty:
        top_volume = []
    else:
        volume_frame["total_volume"] = pd.to_numeric(
            volume_frame["total_volume"],
            errors="coerce",
        ).fillna(0.0)
        top_volume = [
            {
                "symbol": str(row.symbol),
                "total_volume": float(row.total_volume),
            }
            for row in volume_frame.sort_values(
                ["total_volume", "symbol"],
                ascending=[False, True],
            ).head(10).itertuples(index=False)
        ]
    activity_frame = pd.DataFrame(activity_rows)
    if activity_frame.empty:
        return top_volume, [], 1
    activity_frame["second_value"] = pd.to_datetime(
        activity_frame["second"],
        errors="coerce",
        utc=True,
    )
    activity_frame["events"] = pd.to_numeric(
        activity_frame["events"],
        errors="coerce",
    ).fillna(0).astype(int)
    activity_frame = activity_frame.dropna(subset=["second_value"])
    if activity_frame.empty:
        return top_volume, [], 1
    elapsed_seconds = (
        activity_frame["second_value"].max()
        - activity_frame["second_value"].min()
    ).total_seconds()
    if elapsed_seconds <= 30:
        activity_interval = 1
    elif elapsed_seconds <= 120:
        activity_interval = 5
    elif elapsed_seconds <= 900:
        activity_interval = 30
    elif elapsed_seconds <= 3600:
        activity_interval = 60
    else:
        activity_interval = 300
    presentation_timezone = globals().get("PRESENTATION_TIMEZONE", "UTC")
    activity_frame["second_value"] = (
        activity_frame["second_value"]
        .dt.tz_convert(presentation_timezone)
        .dt.floor(f"{activity_interval}s")
    )
    activity_frame = activity_frame.groupby(
        ["second_value", "event_type"],
        as_index=False,
    )["events"].sum().sort_values(
        ["second_value", "event_type"],
        ascending=[True, True],
    )
    prepared_activity = [
        {
            "event_type": str(row.event_type),
            "events": int(row.events),
            "second": row.second_value.isoformat(),
        }
        for row in activity_frame.itertuples(index=False)
    ]
    return top_volume, prepared_activity, activity_interval

def _build_answer(
    frame: pd.DataFrame,
    metric_column: str,
    question: str,
    feed: str,
) -> dict[str, Any]:
    """Construyo una respuesta sin interpretar datos insuficientes."""

    eligible = frame[
        frame[metric_column].notna()
        & frame["trade_count"].notna()
        & (frame["trade_count"] > 0)
    ][["trade_count", metric_column]]
    symbol_count = len(eligible)
    correlation = None
    if (
        symbol_count >= 2
        and eligible["trade_count"].nunique() > 1
        and eligible[metric_column].nunique() > 1
    ):
        correlation = float(eligible["trade_count"].corr(eligible[metric_column]))
    if correlation is None or math.isnan(correlation):
        conclusion = f"{question} Datos insuficientes. No implica causalidad. Feed observado: {feed}."
    elif correlation > 0:
        conclusion = f"{question} Asociación positiva observada; no implica causalidad. Feed observado: {feed}."
    elif correlation < 0:
        conclusion = f"{question} Asociación negativa observada; no implica causalidad. Feed observado: {feed}."
    else:
        conclusion = f"{question} No se observa asociación lineal. No implica causalidad. Feed observado: {feed}."
    return {
        "conclusion": conclusion,
        "correlation": correlation,
        "feed": feed,
        "question_id": "volatility" if metric_column == "volatility" else "spread",
        "symbol_count": symbol_count,
    }

def _float_or_none(value: Any) -> float | None:
    """Convierto valores Pandas a números SQL o nulos."""

    if pd.isna(value):
        return None
    return float(value)

def _integer_or_none(value: Any) -> int | None:
    """Convierto contadores Pandas a enteros SQL o nulos."""

    if pd.isna(value):
        return None
    return int(value)

def _mean_spread(row: Any) -> float | None:
    """Calculo el spread medio desde suma y cantidad válidas."""

    if pd.isna(row.spread_count) or row.spread_count <= 0:
        return None
    return float(row.spread_sum) / float(row.spread_count)

def _sample_standard_deviation(row: Any) -> float | None:
    """Calculo la desviación estándar desde estadísticas suficientes."""

    if pd.isna(row.return_count) or row.return_count < 2:
        return None
    variance = (
        float(row.return_sum_squares)
        - (float(row.return_sum) ** 2 / float(row.return_count))
    ) / float(row.return_count - 1)
    return math.sqrt(max(variance, 0.0))
