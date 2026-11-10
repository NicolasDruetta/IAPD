import math
import pandas as pd

from app import functions
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

DATA_DIRECTORY = Path(__file__).parent.parent / "data"
FEED_NAME = "IEX"
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")
_ANALYSIS_STATES: dict[str, dict[str, Any]] = {}

def analyze(
    run_id: str,
    feed: str = FEED_NAME,
) -> dict[str, Any] | None:
    """Leo lotes Parquet nuevos y preparo resultados acumulados."""

    parquet_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.parts"
    batch_paths = sorted(parquet_path.glob("batch_*")) if parquet_path.is_dir() else []
    if not batch_paths:
        return None
    try:
        state = _ANALYSIS_STATES.get(run_id)
        if state is None:
            state = _create_analysis_state()
            _ANALYSIS_STATES[run_id] = state
            new_paths = batch_paths
        else:
            known_batches = state["batches"]
            new_paths = [path for path in batch_paths if path.name not in known_batches]
        if not new_paths:
            return state["analysis"]
        frame = (
            pd.read_parquet(new_paths[0])
            if len(new_paths) == 1
            else pd.concat(
                [pd.read_parquet(path) for path in new_paths],
                ignore_index=True,
            )
        )
        _update_analysis_state(state, frame)
        metrics_frame = _build_metrics_frame(state)
        if metrics_frame.empty:
            return None
        metrics, answers = _build_analysis_results(metrics_frame, feed)
        top_volume, activity, activity_interval = prepare_chart_data(state)
        analysis = {
            "activity": activity,
            "activity_interval": activity_interval,
            "answers": answers,
            "metrics": metrics,
            "top_volume": top_volume,
        }
        state["analysis"] = analysis
        state["batches"].update(path.name for path in new_paths)
        functions.append_console_log(
            run_id,
            "pandas",
            f"PARQUET | lotes_acumulados={len(state['batches'])} | lotes_nuevos={len(new_paths)} | registros_nuevos={len(frame)}",
        )
        functions.append_console_log(
            run_id,
            "pandas",
            f"PANDAS | análisis actualizado | símbolos={len(metrics)} | correlaciones={len(answers)} | feed={feed}",
        )
        return analysis
    except (OSError, TypeError, ValueError) as error:
        _ANALYSIS_STATES.pop(run_id, None)
        functions.append_console_log(
            run_id,
            "pandas",
            f"ERROR | análisis={error}",
        )
        return None

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
            feed,
        ),
        _build_answer(
            metrics_frame,
            "spread_mean",
            feed,
        ),
    ]
    return metrics, answers

def _build_answer(
    frame: pd.DataFrame,
    metric_column: str,
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
    return {
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

    return {"activity": {}, "analysis": None, "batches": set(), "metrics": {}}

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
