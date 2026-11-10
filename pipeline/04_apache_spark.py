# Autor: Nicolás Druetta

import json
import os
import shutil
import sys
import threading
import time

from app import functions
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col,count,from_json)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from typing import Any, Callable
from zoneinfo import ZoneInfo

_FATAL_ERROR_CALLBACK: Callable[[str, str], None] | None = None
_RUN_STATES: dict[str, dict[str, Any]] = {}
_SPARK_ERROR: str | None = None
_SPARK_LOCK = threading.RLock()
_SPARK_SESSION: SparkSession | None = None
_STARTING_RUNS: set[str] = set()

EVENT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("received_at", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("size", DoubleType(), True),
        StructField("bid_price", DoubleType(), True),
        StructField("bid_size", DoubleType(), True),
        StructField("ask_price", DoubleType(), True),
        StructField("ask_size", DoubleType(), True),
    ]
)
KAFKA_BOOTSTRAP_SERVERS = (
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").strip()
    or "kafka:9092"
)
KAFKA_STARTING_OFFSET_LOOKBACK_SECONDS = 5
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iapd-market-events")
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")
SPARK_APP_NAME = "Apache Big Data Lab"
SPARK_BLOCK_MANAGER_PORT = os.getenv("SPARK_BLOCK_MANAGER_PORT", "4042")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "spark-driver")
SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT", "4041")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

def configure_failure_callback(
    callback: Callable[[str, str], None] | None,
) -> None:
    """Registro el callback único para errores fatales del RUN."""

    global _FATAL_ERROR_CALLBACK
    _FATAL_ERROR_CALLBACK = callback

def _notify_fatal_error(run_id: str, error: str) -> None:
    """Notifico una sola vez un error que impide continuar el RUN."""

    with _SPARK_LOCK:
        state = _RUN_STATES.get(run_id)
        if state is None or state.get("failure_reported"):
            return
        state["failure_reported"] = True
    if _FATAL_ERROR_CALLBACK is not None:
        _FATAL_ERROR_CALLBACK(run_id, error)

def get_checkpoint(run_id: str | None) -> dict[str, Any] | None:
    """Leo el checkpoint estructurado de un RUN."""

    if run_id is None:
        return None
    path = DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_info.jsonl"
    if not path.exists():
        return None
    checkpoint = None
    try:
        with path.open(encoding="utf-8") as info_file:
            for line in info_file:
                record = json.loads(line)
                if record.get("record_type") == "spark_checkpoint":
                    checkpoint = record
        return checkpoint
    except (OSError, json.JSONDecodeError):
        return None

def get_spark_session() -> SparkSession | None:
    """Obtengo una única SparkSession distribuida reutilizable."""

    global _SPARK_ERROR, _SPARK_SESSION
    with _SPARK_LOCK:
        if _SPARK_SESSION is None:
            existing_session = (
                SparkSession._instantiatedSession
                or SparkSession._activeSession
            )
            if existing_session is not None:
                _SPARK_SESSION = existing_session
        if _SPARK_SESSION is not None:
            try:
                is_stopped = _SPARK_SESSION.sparkContext._jsc.sc().isStopped()
            except Exception as error:
                _SPARK_ERROR = f"No se pudo consultar Spark: {error}"
                return _SPARK_SESSION
            else:
                if not is_stopped:
                    return _SPARK_SESSION
                reset_spark_runtime()
        try:
            builder = (
                SparkSession.builder
                .master(SPARK_MASTER)
                .appName(SPARK_APP_NAME)
                .config("spark.sql.shuffle.partitions", "2")
            )
            spark_jars = ",".join(
                str(path)
                for path in Path("/opt/spark-jars").glob("*.jar")
            )
            if spark_jars:
                builder = builder.config("spark.jars", spark_jars)
            if SPARK_MASTER.startswith("spark://"):
                builder = (
                    builder
                    .config("spark.driver.host", SPARK_DRIVER_HOST)
                    .config("spark.driver.bindAddress", "0.0.0.0")
                    .config("spark.driver.port", SPARK_DRIVER_PORT)
                    .config("spark.blockManager.port", SPARK_BLOCK_MANAGER_PORT)
                    .config("spark.executor.cores", "1")
                    .config("spark.cores.max", "2")
                    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
                    .config("spark.driver.maxResultSize", "128m")
                    .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "512m"))
                    .config("spark.executor.memoryOverhead", "256m")
                )
            _SPARK_SESSION = builder.getOrCreate()
            _SPARK_SESSION.sparkContext.setLogLevel("WARN")
        except Exception as error:
            _SPARK_ERROR = f"No se pudo iniciar Spark: {error}"
        return _SPARK_SESSION

def has_active_query() -> bool:
    """Indico si existe una query Structured Streaming activa en este módulo."""

    with _SPARK_LOCK:
        queries = [
            state.get("query")
            for state in _RUN_STATES.values()
            if state.get("query") is not None
        ]
    for query in queries:
        try:
            if query.isActive:
                return True
        except Exception:
            return False
    return False

def reset_spark_runtime() -> None:
    """Libero una sesión cuyo gateway JVM dejó de responder."""

    global _SPARK_ERROR, _SPARK_SESSION
    gateway = getattr(SparkContext, "_gateway", None)
    session = _SPARK_SESSION
    _SPARK_SESSION = None
    _SPARK_ERROR = None
    try:
        if session is not None:
            session.stop()
    except Exception:
        pass
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    try:
        if gateway is not None:
            gateway.shutdown()
    except Exception:
        pass
    SparkContext._gateway = None
    SparkContext._jvm = None
    SparkContext._active_spark_context = None

def process_batch(run_id: str, data_frame: DataFrame, batch_id: int) -> None:
    """Proceso un microbatch distribuido y escribo su fragmento Parquet."""

    input_frame = data_frame.persist()
    processed_frame = None
    try:
        input_count = input_frame.count()
        if not input_count:
            return
        functions.append_console_log(
            run_id,
            "spark",
            f"EXTRACT | batch_id={batch_id} | eventos={input_count} | origen=Kafka",
        )
        valid_frame = input_frame.where(
            col("run_id").isNotNull()
            & col("timestamp").isNotNull()
            & col("symbol").isNotNull()
            & col("event_type").isin("TRADE", "QUOTE")
        )
        valid_count = valid_frame.count()
        processed_frame = valid_frame.dropDuplicates([
            "run_id", "timestamp", "symbol", "event_type", "price", "size",
            "bid_price", "bid_size", "ask_price", "ask_size",
        ]).persist()
        processed_count = processed_frame.count()
        if not processed_count:
            return
        parts_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.parts"
        batch_path = parts_path / f"batch_{batch_id:020d}"
        temporary_path = parts_path.parent / f".{batch_path.name}.tmp"
        parts_path.mkdir(parents=True, exist_ok=True)
        if not batch_path.exists():
            shutil.rmtree(temporary_path, ignore_errors=True)
            processed_frame.write.mode("overwrite").parquet(
                str(temporary_path)
            )
            os.replace(temporary_path, batch_path)
        analysis_module = sys.modules.get("06_postgresql")
        if analysis_module is None:
            raise RuntimeError("PostgreSQL no está disponible para persistir el batch.")
        if not analysis_module.analyze_and_persist(run_id):
            raise RuntimeError("El análisis o la persistencia del batch no se completó.")
        trade_count = processed_frame.where(
            col("event_type") == "TRADE"
        ).count()
        quote_count = processed_frame.where(
            col("event_type") == "QUOTE"
        ).count()
        symbol_count = processed_frame.select("symbol").distinct().count()
        with _SPARK_LOCK:
            state = _RUN_STATES[run_id]
            state["input_events"] += input_count
            state["processed_events"] += processed_count
            state["processed_events_by_type"]["TRADE"] += trade_count
            state["processed_events_by_type"]["QUOTE"] += quote_count
            state["batch_number"] += 1
            state["status"] = "completado"
            state["checkpoint"] = {
                "input_events": state["input_events"],
                "run_id": run_id,
                "processed_events": state["processed_events"],
                "batch_number": state["batch_number"],
                "processed_events_by_type": dict(
                    state["processed_events_by_type"]
                ),
            }
            checkpoint = dict(state["checkpoint"])
        info_path = DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_info.jsonl"
        with info_path.open("a", encoding="utf-8") as info_file:
            info_file.write(json.dumps(
                {"record_type": "spark_checkpoint", **checkpoint}, ensure_ascii=False
            ) + "\n")
            info_file.flush()
            os.fsync(info_file.fileno())
        for message in (
            f"TRANSFORM | batch_id={batch_id} | entrada={input_count} | validos={valid_count} | invalidos={input_count - valid_count} | duplicados_eliminados={valid_count - processed_count}",
            f"LOAD | batch_id={batch_id} | ruta={parts_path} | registros={processed_count} | estado=completado",
            f"EDA | batch_id={batch_id} | simbolos={symbol_count} | trades={checkpoint['processed_events_by_type']['TRADE']} | quotes={checkpoint['processed_events_by_type']['QUOTE']}",
        ):
            functions.append_console_log(run_id, "spark", message)
    except Exception as error:
        with _SPARK_LOCK:
            stopping = bool(
                _RUN_STATES.get(run_id, {}).get("stopping", False)
            )
        if stopping or "InterruptedException" in str(error):
            functions.append_console_log(
                run_id,
                "spark",
                f"STOP | batch_id={batch_id} | microbatch interrumpido",
            )
            return
        functions.append_console_log(
            run_id, "spark", f"ERROR | batch_id={batch_id} | procesamiento={error}"
        )
        _notify_fatal_error(
            run_id,
            f"error en microbatch {batch_id}: {error}",
        )
        raise
    finally:
        input_frame.unpersist()
        if processed_frame is not None:
            processed_frame.unpersist()

def initialize_parquet_output(run_id: str, spark: SparkSession) -> None:
    """Creo el Parquet vacío y el estado visible del RUN."""

    runs_path = DATA_DIRECTORY / "runs"
    runs_path.mkdir(parents=True, exist_ok=True)
    final_path = runs_path / f"{run_id}_04_apache_spark_data.parquet"
    status_path = runs_path / f"{run_id}_04_apache_spark_data_status.txt"
    if not final_path.exists():
        temporary_path = DATA_DIRECTORY / "spark" / f"{run_id}_empty_parquet.tmp"
        temporary_file = runs_path / f".{final_path.name}.empty.tmp"
        shutil.rmtree(temporary_path, ignore_errors=True)
        spark.createDataFrame([], EVENT_SCHEMA).coalesce(1).write.mode(
            "overwrite"
        ).parquet(str(temporary_path))
        part_files = sorted(temporary_path.glob("part-*.parquet"))
        if len(part_files) != 1:
            raise RuntimeError(
                f"La inicialización de {run_id} generó {len(part_files)} archivos."
            )
        shutil.copy2(part_files[0], temporary_file)
        os.replace(temporary_file, final_path)
        shutil.rmtree(temporary_path)
    status_path.write_text(
        "Parquet en proceso. Los datos se están escribiendo temporalmente en "
        f"`/app/data/spark/{run_id}_04_apache_spark_data.parquet.parts/`. "
        "El archivo definitivo se consolidará aquí cuando finalice la ejecución.",
        encoding="utf-8",
    )

def consolidate_parquet(run_id: str) -> bool:
    """Consolido una sola vez los fragmentos Parquet del RUN."""

    parts_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.parts"
    final_path = DATA_DIRECTORY / "runs" / f"{run_id}_04_apache_spark_data.parquet"
    status_path = DATA_DIRECTORY / "runs" / f"{run_id}_04_apache_spark_data_status.txt"
    temporary_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.tmp"
    if not parts_path.exists() or not any(parts_path.glob("batch_*")):
        status_path.unlink(missing_ok=True)
        return final_path.exists()
    functions.append_console_log(
        run_id, "spark", "SPARK | iniciando sesión y consulta Structured Streaming"
    )
    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark no está disponible para consolidar Parquet.")
    temporary_file = DATA_DIRECTORY / "runs" / f".{final_path.name}.tmp"
    try:
        processed_frame = (
            spark.read
            .option("recursiveFileLookup", "true")
            .parquet(str(parts_path))
            .dropDuplicates([
                "run_id", "timestamp", "symbol", "event_type", "price", "size",
                "bid_price", "bid_size", "ask_price", "ask_size",
            ])
        )
        shutil.rmtree(temporary_path, ignore_errors=True)
        processed_frame.coalesce(1).write.mode("overwrite").parquet(
            str(temporary_path)
        )
        part_files = sorted(temporary_path.glob("part-*.parquet"))
        if len(part_files) != 1:
            raise RuntimeError(
                f"La consolidación de {run_id} generó {len(part_files)} archivos."
            )
        shutil.copy2(part_files[0], temporary_file)
        os.replace(temporary_file, final_path)
        shutil.rmtree(temporary_path)
        shutil.rmtree(parts_path)
        status_path.unlink(missing_ok=True)
        return True
    except Exception:
        temporary_file.unlink(missing_ok=True)
        status_path.write_text(
            "Consolidación pendiente. Los fragmentos Parquet permanecen en "
            f"`/app/data/spark/{run_id}_04_apache_spark_data.parquet.parts/`.",
            encoding="utf-8",
        )
        raise

def start_streaming(run_id: str) -> None:
    """Inicio Structured Streaming Kafka para un RUN sin duplicar queries."""

    with _SPARK_LOCK:
        existing = _RUN_STATES.get(run_id)
        if existing is not None and existing["query"] is not None:
            if existing["query"].isActive:
                return
            query_error = existing["query"].exception()
            functions.append_console_log(
                run_id,
                "spark",
                f"ERROR | Structured Streaming | query={query_error}",
            )
            raise RuntimeError(
                f"La consulta Structured Streaming terminó: {query_error}"
            )
        if run_id in _STARTING_RUNS:
            return
        _STARTING_RUNS.add(run_id)
        checkpoint = get_checkpoint(run_id) or {
            "processed_events": 0,
            "batch_number": 0,
            "processed_events_by_type": {"TRADE": 0, "QUOTE": 0},
        }
        _RUN_STATES[run_id] = {
            "batch_number": int(checkpoint.get("batch_number", 0)),
            "checkpoint": checkpoint,
            "input_events": int(
                checkpoint.get(
                    "input_events",
                    checkpoint.get("processed_events", 0),
                )
            ),
            "processed_events": int(checkpoint.get("processed_events", 0)),
            "processed_events_by_type": dict(checkpoint.get("processed_events_by_type", {})),
            "query": None,
            "query_id": None,
            "status": "iniciando",
            "stopping": False,
            "failure_reported": False,
        }
    spark = get_spark_session()
    if spark is None:
        error_message = _SPARK_ERROR or "Spark no disponible."
        with _SPARK_LOCK:
            _STARTING_RUNS.discard(run_id)
            _RUN_STATES[run_id]["status"] = "error"
        functions.append_console_log(run_id, "spark", error_message)
        raise RuntimeError(error_message)
    (DATA_DIRECTORY / "spark").mkdir(parents=True, exist_ok=True)
    checkpoint_path = str(DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_checkpoint")
    starting_timestamp = (
        datetime.strptime(run_id, "%Y%m%d_%H%M%S")
        .replace(tzinfo=PRESENTATION_TIMEZONE)
        .astimezone(timezone.utc)
        - timedelta(seconds=KAFKA_STARTING_OFFSET_LOOKBACK_SECONDS)
    )
    starting_timestamp_milliseconds = int(starting_timestamp.timestamp() * 1000)
    try:
        initialize_parquet_output(run_id, spark)
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingTimestamp", starting_timestamp_milliseconds)
            .option("startingOffsetsByTimestampStrategy", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
            .select("event.*")
            .where(col("run_id") == run_id)
        )
        query = (
            events.writeStream.foreachBatch(
                lambda data_frame, batch_id: process_batch(run_id, data_frame, batch_id)
            )
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .start()
        )
    except Exception:
        with _SPARK_LOCK:
            _STARTING_RUNS.discard(run_id)
        raise
    with _SPARK_LOCK:
        _STARTING_RUNS.discard(run_id)
        _RUN_STATES[run_id]["query"] = query
        _RUN_STATES[run_id]["query_id"] = str(query.id)

def stop_streaming(
    run_id: str | None,
    finalize: bool = False,
) -> bool:
    """Detengo la consulta del RUN y consolido solo cuando corresponde."""

    if run_id is None:
        return False
    with _SPARK_LOCK:
        state = _RUN_STATES.get(run_id)
        if state is None:
            return True
        state["stopping"] = True
        query = state.get("query") if state else None
    if query is not None:
        try:
            if query.isActive:
                query.stop()
            # Spark puede devolver de query.stop() antes de que el driver
            # haya liberado la consulta. Esperamos un margen corto para que
            # no queden microbatches ni foreachBatch ejecutándose detrás del STOP.
            deadline = time.monotonic() + 5
            while query.isActive and time.monotonic() < deadline:
                time.sleep(0.1)
        except Exception as error:
            error_text = str(error)
            if "stopped SparkContext" not in error_text:
                functions.append_console_log(
                    run_id,
                    "spark",
                    f"ERROR | STOP | cierre Spark: {error}",
                )
                return False
            with _SPARK_LOCK:
                state = _RUN_STATES.get(run_id)
                if state is not None:
                    state["query"] = None
    if finalize:
        try:
            consolidated = consolidate_parquet(run_id)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "spark",
                f"ERROR | consolidación Parquet: {error}",
            )
            _notify_fatal_error(
                run_id,
                f"error de consolidación Parquet: {error}",
            )
            if not has_active_query():
                reset_spark_runtime()
            with _SPARK_LOCK:
                _RUN_STATES.pop(run_id, None)
            return False
        if not consolidated:
            functions.append_console_log(
                run_id,
                "spark",
                "ERROR | consolidación Parquet no completada; "
                "conservo temporales y checkpoint.",
            )
        if not has_active_query():
            reset_spark_runtime()
        with _SPARK_LOCK:
            _RUN_STATES.pop(run_id, None)
        return consolidated
    if not has_active_query():
        reset_spark_runtime()
    with _SPARK_LOCK:
        _RUN_STATES.pop(run_id, None)
    return True

