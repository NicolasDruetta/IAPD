"""Procesamiento local de lotes de operaciones mediante Apache Spark."""

import os
import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as sum_rows
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from typing import Any

SPARK_APP_NAME = "Apache Big Data Lab"
SPARK_MASTER = os.getenv(
    "SPARK_MASTER_URL",
    "spark://spark-master:7077",
)
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "spark-driver")
SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT", "4041")
SPARK_BLOCK_MANAGER_PORT = os.getenv(
    "SPARK_BLOCK_MANAGER_PORT",
    "4042",
)
MAX_BATCH_SIZE = 100

_SPARK_LOCK = threading.RLock()
_SPARK_SESSION: Any = None
_SPARK_ERROR: str | None = None
_LAST_BATCH_KEY: tuple[Any, ...] | None = None
_LAST_RESULT: dict[str, Any] = {
    "status": "sin_datos",
    "logs": ["Spark | esperando un lote de operaciones."],
    "rows": [],
}

def get_snapshot() -> dict[str, Any]:
    """Devuelvo el último resultado real del procesamiento Spark."""

    with _SPARK_LOCK:
        return {
            "status": _LAST_RESULT["status"],
            "logs": list(_LAST_RESULT["logs"]),
            "rows": list(_LAST_RESULT["rows"]),
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

def process_events(events: list[Any]) -> dict[str, Any]:
    """Proceso un lote acotado y evito repetir el mismo job."""

    global _LAST_BATCH_KEY, _LAST_RESULT
    batch = events[-MAX_BATCH_SIZE:]
    batch_key = tuple(
        (event.symbol, event.timestamp, event.price, event.size)
        for event in batch
    )
    with _SPARK_LOCK:
        if batch_key == _LAST_BATCH_KEY:
            return _LAST_RESULT
        _LAST_BATCH_KEY = batch_key
    if not batch:
        _LAST_RESULT = {
            "status": "sin_datos",
            "logs": ["Spark | esperando un lote de operaciones."],
            "rows": [],
        }
        return _LAST_RESULT
    spark = get_spark_session()
    if spark is None:
        _LAST_RESULT = {
            "status": "no_disponible",
            "logs": [
                "Spark | no disponible.",
                _SPARK_ERROR or "Java 17 o superior es requerido.",
            ],
            "rows": [],
        }
        return _LAST_RESULT
    rows = [
        {
            "symbol": event.symbol,
            "price": event.price,
            "size": event.size,
            "timestamp": event.timestamp,
        }
        for event in batch
    ]
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), True),
            StructField("size", DoubleType(), True),
            StructField("timestamp", StringType(), False),
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
                count("*").alias("operations"),
                sum_rows("size").alias("total_volume"),
            )
            .orderBy("symbol")
        )
        result_rows = [row.asDict() for row in grouped.collect()]
        duration = time.perf_counter() - started_at
        partitions = data_frame.rdd.getNumPartitions()
        _LAST_RESULT = {
            "status": "completado",
            "logs": [
                f"Spark | master: {SPARK_MASTER}",
                f"Spark | registros procesados={len(batch)}",
                f"Spark | particiones utilizadas={partitions}",
                f"Spark | duración={duration:.4f} s",
                "Spark | agrupación por símbolo completada.",
            ],
            "rows": result_rows,
        }
    except Exception as error:
        _LAST_RESULT = {
            "status": "error",
            "logs": [f"Spark | error durante el procesamiento: {error}"],
            "rows": [],
        }
    return _LAST_RESULT
