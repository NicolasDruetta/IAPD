import functions
import math

import pandas as pd
import psycopg

from pathlib import Path
from psycopg.types.json import Jsonb
from typing import Any
from zoneinfo import ZoneInfo

DATA_DIRECTORY = Path(__file__).parent.parent / "data"
FEED_NAME = "IEX"
MARKET_ANSWERS_TABLE = "market_answers"
MARKET_METRICS_TABLE = "market_metrics"
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")
_ANALYSIS_STATES: dict[str, dict[str, Any]] = {}

def analyze_and_persist(
    run_id: str,
    feed: str = FEED_NAME,
) -> bool:
    """Leo solo los lotes Parquet nuevos y guardo sus resultados acumulados."""

    parquet_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.parts"
    batch_paths = sorted(parquet_path.glob("batch_*")) if parquet_path.is_dir() else []
    if not batch_paths:
        return False
    state = _ANALYSIS_STATES.get(run_id)
    if state is None:
        frame = pd.read_parquet(parquet_path)
        state = _create_analysis_state()
        _update_analysis_state(state, frame)
        state["batches"] = {path.name for path in batch_paths}
    else:
        known_batches = state["batches"]
        new_paths = [path for path in batch_paths if path.name not in known_batches]
        if not new_paths:
            return True
        frame = pd.concat(
            [pd.read_parquet(path) for path in new_paths],
            ignore_index=True,
        )
        _update_analysis_state(state, frame)
        known_batches.update(path.name for path in new_paths)
    metrics_frame = _build_metrics_frame(state)
    if metrics_frame.empty:
        return False
    metrics, answers = _build_analysis_results(metrics_frame, feed)
    top_volume, activity, activity_interval = prepare_chart_data(state)
    try:
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
            f"PARQUET | lotes={len(state['batches'])} | nuevos={len(frame)}",
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
    state: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Preparo los gráficos desde acumulados, sin conservar eventos crudos."""

    metrics = state["metrics"]
    top_volume = [
        {
            "symbol": symbol,
            "total_volume": float(values["total_volume"]),
        }
        for symbol, values in sorted(
            metrics.items(),
            key=lambda item: (-item[1]["total_volume"], item[0]),
        )[:10]
    ]
    activity_frame = pd.DataFrame(state["activity"].values())
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
    activity_frame["second_value"] = (
        activity_frame["second_value"]
        .dt.tz_convert(PRESENTATION_TIMEZONE)
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

def _build_analysis_results(
    metrics_frame: pd.DataFrame,
    feed: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Construyo métricas y respuestas desde acumulados Pandas."""

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
        for row in metrics_frame.itertuples(index=False)
    ]
    answers = [
        _build_answer(
            metrics_frame,
            "volatility",
            "Las acciones con más operaciones presentan mayor volatilidad.",
            feed,
        ),
        _build_answer(
            metrics_frame,
            "spread_mean",
            "Las acciones más negociadas tienen menor spread entre compra y venta.",
            feed,
        ),
    ]
    return metrics, answers

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

def _build_metrics_frame(
    state: dict[str, Any],
) -> pd.DataFrame:
    """Construyo la tabla financiera desde acumulados suficientes."""

    rows = []
    for symbol, values in sorted(state["metrics"].items()):
        return_count = values["return_count"]
        volatility = None
        if return_count >= 2:
            variance = (
                values["return_sum_squares"]
                - values["return_sum"] ** 2 / return_count
            ) / (return_count - 1)
            volatility = math.sqrt(max(variance, 0.0))
        spread_mean = (
            values["spread_sum"] / values["spread_count"]
            if values["spread_count"]
            else None
        )
        rows.append(
            {
                "return_count": return_count,
                "spread_count": values["spread_count"],
                "spread_mean": spread_mean,
                "symbol": symbol,
                "total_volume": values["total_volume"],
                "trade_count": values["trade_count"],
                "volatility": volatility,
            }
        )
    return pd.DataFrame(rows)

def _create_analysis_state() -> dict[str, Any]:
    """Creo el estado pequeño de acumulados, no de eventos crudos."""

    return {"activity": {}, "batches": set(), "metrics": {}}

def _prepare_events(frame: pd.DataFrame) -> pd.DataFrame:
    """Preparo timestamps y tipos numéricos del microbatch Parquet."""

    prepared = frame.copy()
    prepared["timestamp_value"] = pd.to_datetime(
        prepared["timestamp"],
        errors="coerce",
        utc=True,
    )
    prepared["received_at_value"] = pd.to_datetime(
        prepared["received_at"],
        errors="coerce",
        utc=True,
    )
    for column in [
        "price",
        "size",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    ]:
        prepared[column] = pd.to_numeric(
            prepared[column],
            errors="coerce",
        )
    prepared = prepared[
        prepared["timestamp_value"].notna()
        & prepared["symbol"].notna()
        & prepared["event_type"].isin(["TRADE", "QUOTE"])
    ].copy()
    return prepared.sort_values(
        ["symbol", "timestamp_value", "received_at_value", "price", "size"],
        na_position="last",
    )

def _update_analysis_state(
    state: dict[str, Any],
    frame: pd.DataFrame,
) -> None:
    """Actualizo acumulados financieros y temporales con un microbatch."""

    prepared = _prepare_events(frame)
    if prepared.empty:
        return
    for symbol, symbol_frame in prepared.groupby("symbol", sort=True):
        values = state["metrics"].setdefault(
            str(symbol),
            {
                "last_trade_price": None,
                "return_count": 0,
                "return_sum": 0.0,
                "return_sum_squares": 0.0,
                "spread_count": 0,
                "spread_sum": 0.0,
                "total_volume": 0.0,
                "trade_count": 0,
            },
        )
        trades = symbol_frame[symbol_frame["event_type"] == "TRADE"]
        for row in trades.itertuples(index=False):
            values["trade_count"] += 1
            values["total_volume"] += float(row.size) if pd.notna(row.size) else 0.0
            if pd.notna(row.price) and row.price > 0:
                previous_price = values["last_trade_price"]
                if previous_price is not None and previous_price > 0:
                    return_value = float(row.price) / previous_price - 1
                    values["return_count"] += 1
                    values["return_sum"] += return_value
                    values["return_sum_squares"] += return_value * return_value
                values["last_trade_price"] = float(row.price)
        quotes = symbol_frame[
            (symbol_frame["event_type"] == "QUOTE")
            & (symbol_frame["bid_price"] > 0)
            & (symbol_frame["ask_price"] > 0)
            & (symbol_frame["ask_price"] >= symbol_frame["bid_price"])
        ].copy()
        if not quotes.empty:
            mid_price = (quotes["ask_price"] + quotes["bid_price"]) / 2
            spread_values = (
                (quotes["ask_price"] - quotes["bid_price"]) / mid_price
            )
            values["spread_count"] += int(spread_values.count())
            values["spread_sum"] += float(spread_values.sum())
    activity = (
        prepared.assign(second=prepared["timestamp_value"].dt.floor("s"))
        .groupby(["second", "event_type"], as_index=False)
        .size()
        .rename(columns={"size": "events"})
    )
    for row in activity.itertuples(index=False):
        second = row.second.isoformat()
        key = f"{second}|{row.event_type}"
        previous = state["activity"].get(
            key,
            {"event_type": str(row.event_type), "events": 0, "second": second},
        )
        previous["events"] += int(row.events)
        state["activity"][key] = previous

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
