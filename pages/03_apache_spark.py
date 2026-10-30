"""Procesamiento distribuido de cotizaciones y operaciones con Spark."""

import os
import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as sum_rows, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from typing import Any

SPARK_APP_NAME = "Apache Big Data Lab"
SPARK_BLOCK_MANAGER_PORT = os.getenv(
    "SPARK_BLOCK_MANAGER_PORT",
    "4042",
)
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "spark-driver")
SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT", "4041")
SPARK_MASTER = os.getenv(
    "SPARK_MASTER_URL",
    "spark://spark-master:7077",
)
_BATCH_NUMBER = 0
_LAST_RESULT: dict[str, Any] = {
    "status": "sin_datos",
    "logs": ["Spark | esperando eventos bursátiles."],
    "rows": [],
    "top_volume": [],
    "checkpoint": None,
}
_RUN_STATES: dict[str, dict[str, Any]] = {}
_SPARK_ERROR: str | None = None
_SPARK_LOCK = threading.RLock()
_SPARK_SESSION: Any = None

def get_snapshot() -> dict[str, Any]:
    """Devuelvo el último resultado real del procesamiento Spark."""

    with _SPARK_LOCK:
        return {
            "status": _LAST_RESULT["status"],
            "logs": list(_LAST_RESULT["logs"]),
            "rows": list(_LAST_RESULT["rows"]),
            "top_volume": list(_LAST_RESULT["top_volume"]),
            "checkpoint": _LAST_RESULT["checkpoint"],
        }

def get_spark_session() -> Any:
    """Obtengo una única SparkSession local reutilizable."""

    global _SPARK_ERROR, _SPARK_SESSION
    with _SPARK_LOCK:
        if _SPARK_SESSION is not None:
            return _SPARK_SESSION
        if _SPARK_ERROR is not None:
            return None
        try:
            builder = (
                SparkSession.builder
                .master(SPARK_MASTER)
                .appName(SPARK_APP_NAME)
            )
            if SPARK_MASTER.startswith("spark://"):
                builder = (
                    builder
                    .config("spark.driver.host", SPARK_DRIVER_HOST)
                    .config("spark.driver.bindAddress", "0.0.0.0")
                    .config("spark.driver.port", SPARK_DRIVER_PORT)
                    .config(
                        "spark.blockManager.port",
                        SPARK_BLOCK_MANAGER_PORT,
                    )
                    .config("spark.executor.cores", "1")
                    .config("spark.cores.max", "2")
                    .config(
                        "spark.driver.memory",
                        os.getenv("SPARK_DRIVER_MEMORY", "1g"),
                    )
                    .config(
                        "spark.executor.memory",
                        os.getenv("SPARK_EXECUTOR_MEMORY", "512m"),
                    )
                    .config("spark.executor.memoryOverhead", "256m")
                )
            _SPARK_SESSION = builder.getOrCreate()
            _SPARK_SESSION.sparkContext.setLogLevel("WARN")
        except Exception as error:
            _SPARK_ERROR = f"No se pudo iniciar Spark: {error}"
        return _SPARK_SESSION

def process_events(
    events: list[Any],
    run_id: str | None = None,
    checkpoint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Proceso el lote nuevo y acumulo volumen confirmado por Spark."""

    global _BATCH_NUMBER, _LAST_RESULT
    state_key = run_id or "__default__"
    with _SPARK_LOCK:
        state = _RUN_STATES.get(state_key)
        if state is None:
            state = {
                "processed_events": int(
                    (checkpoint or {}).get("processed_events", 0)
                ),
                "batch_number": int(
                    (checkpoint or {}).get("batch_number", 0)
                ),
                "trade_totals": {
                    str(symbol): float(volume)
                    for symbol, volume in (checkpoint or {}).get(
                            "trade_totals",
                            (checkpoint or {}).get("totals", {}),
                        ).items()
                },
                "processed_events_by_type": {
                    "TRADE": int(
                        (checkpoint or {}).get(
                            "processed_events_by_type",
                            {},
                        ).get("TRADE", 0)
                    ),
                    "QUOTE": int(
                        (checkpoint or {}).get(
                            "processed_events_by_type",
                            {},
                        ).get("QUOTE", 0)
                    ),
                },
                "totals": {
                    str(symbol): float(volume)
                    for symbol, volume in (checkpoint or {}).get(
                        "trade_totals",
                        (checkpoint or {}).get("totals", {}),
                    ).items()
                },
                "result": None,
            }
            _RUN_STATES[state_key] = state
        elif checkpoint is not None and state["processed_events"] > int(
            checkpoint.get("processed_events", 0)
        ):
            return state["result"] or _LAST_RESULT
    if not events:
        result = state["result"]
        if result is not None:
            return result
        result = {
            "status": "sin_datos",
            "logs": ["Spark | esperando eventos bursátiles."],
            "rows": [],
            "top_volume": top_volume_rows(state["totals"]),
            "checkpoint": spark_checkpoint(state, run_id),
        }
        state["result"] = result
        _LAST_RESULT = result
        return result
    spark = get_spark_session()
    if spark is None:
        result = {
            "status": "no_disponible",
            "logs": [
                "Spark | no disponible.",
                _SPARK_ERROR or "Java 17 o superior es requerido.",
            ],
            "rows": [],
            "top_volume": top_volume_rows(state["totals"]),
            "checkpoint": None,
        }
        _LAST_RESULT = result
        return result
    rows = [
        {
            "symbol": event.symbol,
            "event_type": event.event_type,
            "price": event.price,
            "size": event.size,
            "timestamp": event.timestamp,
            "bid_price": event.bid_price,
            "bid_size": event.bid_size,
            "ask_price": event.ask_price,
            "ask_size": event.ask_size,
        }
        for event in events
    ]
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("price", DoubleType(), True),
            StructField("size", DoubleType(), True),
            StructField("timestamp", StringType(), False),
            StructField("bid_price", DoubleType(), True),
            StructField("bid_size", DoubleType(), True),
            StructField("ask_price", DoubleType(), True),
            StructField("ask_size", DoubleType(), True),
        ]
    )
    started_at = time.perf_counter()
    try:
        data_frame = (
            spark.createDataFrame(rows, schema=schema)
            .repartition(4, "symbol")
        )
        grouped = (
            data_frame.groupBy("symbol")
            .agg(
                count(
                    when(data_frame.event_type == "TRADE", True)
                ).alias("operations"),
                count(
                    when(data_frame.event_type == "QUOTE", True)
                ).alias("quotes"),
                sum_rows(
                    when(data_frame.event_type == "TRADE", data_frame.size)
                ).alias("total_volume"),
            )
            .orderBy("symbol")
        )
        result_rows = [row.asDict() for row in grouped.collect()]
        duration = time.perf_counter() - started_at
        partitions = data_frame.rdd.getNumPartitions()
        batch_totals = {
            str(row["symbol"]): float(row["total_volume"] or 0)
            for row in result_rows
        }
        for symbol, volume in batch_totals.items():
            state["trade_totals"][symbol] = (
                state["trade_totals"].get(symbol, 0.0) + volume
            )
        state["totals"] = state["trade_totals"]
        state["processed_events_by_type"]["TRADE"] += sum(
            event.event_type == "TRADE" for event in events
        )
        state["processed_events_by_type"]["QUOTE"] += sum(
            event.event_type == "QUOTE" for event in events
        )
        trade_count = state["processed_events_by_type"]["TRADE"]
        quote_count = state["processed_events_by_type"]["QUOTE"]
        _BATCH_NUMBER += 1
        state["batch_number"] += 1
        state["processed_events"] += len(events)
        checkpoint_data = spark_checkpoint(state, run_id)
        result = {
            "status": "completado",
            "logs": [
                f"Spark | master: {SPARK_MASTER}",
                f"Spark | lote={state['batch_number']}",
                f"Spark | registros procesados={len(events)}",
                f"Spark | operaciones procesadas={trade_count}",
                f"Spark | cotizaciones procesadas={quote_count}",
                f"Spark | particiones utilizadas={partitions}",
                f"Spark | duración={duration:.4f} s",
                "Spark | agrupación por símbolo completada.",
            ],
            "rows": result_rows,
            "top_volume": top_volume_rows(state["trade_totals"]),
            "checkpoint": checkpoint_data,
        }
        state["result"] = result
        _LAST_RESULT = result
    except Exception as error:
        result = {
            "status": "error",
            "logs": [f"Spark | error durante el procesamiento: {error}"],
            "rows": [],
            "top_volume": top_volume_rows(state["totals"]),
            "checkpoint": None,
        }
        state["result"] = result
        _LAST_RESULT = result
    return _LAST_RESULT

def spark_checkpoint(
    state: dict[str, Any],
    run_id: str | None,
) -> dict[str, Any]:
    """Genero el registro pequeño de recuperación del acumulado."""

    return {
        "run_id": run_id,
        "processed_events": state["processed_events"],
        "batch_number": state["batch_number"],
        "totals": dict(state["trade_totals"]),
        "trade_totals": dict(state["trade_totals"]),
        "processed_events_by_type": dict(state["processed_events_by_type"]),
    }

def top_volume_rows(totals: dict[str, float]) -> list[dict[str, Any]]:
    """Ordeno el ranking acumulado y conservo sus diez primeros símbolos."""

    return [
        {
            "symbol": symbol,
            "total_volume": volume,
        }
        for symbol, volume in sorted(
            totals.items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]
    ]
