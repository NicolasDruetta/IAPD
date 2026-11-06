import ast
import functions
import json
import os
import shutil
import threading
import urllib.request

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_trunc,
    from_json,
    regexp_replace,
    sum as sum_rows,
    when,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_RUN_STATES: dict[str, dict[str, Any]] = {}
_SPARK_ERROR: str | None = None
_SPARK_LOCK = threading.RLock()
_SPARK_SESSION: SparkSession | None = None
_STARTING_RUNS: set[str] = set()

EVENT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("size", DoubleType(), True),
        StructField("bid_price", DoubleType(), True),
        StructField("bid_size", DoubleType(), True),
        StructField("ask_price", DoubleType(), True),
        StructField("ask_size", DoubleType(), True),
        StructField("mode", StringType(), True),
    ]
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iapd-market-events")
SPARK_APP_NAME = "Apache Big Data Lab"
SPARK_BLOCK_MANAGER_PORT = os.getenv("SPARK_BLOCK_MANAGER_PORT", "4042")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "spark-driver")
SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT", "4041")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_MASTER_UI_URL = os.getenv("SPARK_MASTER_UI_URL", "http://spark-master:8080")
SPARK_UI_URL = os.getenv("SPARK_UI_URL", "http://localhost:4040")

def get_checkpoint(run_id: str | None) -> dict[str, Any] | None:
    """Leo el checkpoint estructurado de un RUN."""

    if run_id is None:
        return None
    path = DATA_DIRECTORY / "runs" / f"{run_id}_00_run_info.jsonl"
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

def get_persisted_top_volume(run_id: str | None) -> list[dict[str, Any]]:
    """Leo el último ranking de volumen persistido por Spark."""

    if run_id is None:
        return []
    path = DATA_DIRECTORY / "runs" / f"{run_id}_04_spark_log.txt"
    if not path.exists():
        return []
    marker = "volumen_trade_por_accion="
    try:
        with path.open(encoding="utf-8") as spark_log:
            for line in reversed(spark_log.readlines()):
                if marker not in line:
                    continue
                ranking_text = line.split(marker, 1)[1].strip()
                ranking = ast.literal_eval(ranking_text)
                if not isinstance(ranking, list):
                    return []
                return [
                    {
                        "symbol": str(row["symbol"]),
                        "total_volume": float(row["total_volume"]),
                    }
                    for row in ranking
                    if isinstance(row, dict)
                    and row.get("symbol") is not None
                    and row.get("total_volume") is not None
                ]
    except (OSError, SyntaxError, ValueError, KeyError, TypeError):
        return []
    return []

def get_snapshot(run_id: str | None = None) -> dict[str, Any]:
    """Devuelvo el resultado acumulado de Spark para un RUN."""

    with _SPARK_LOCK:
        state = _RUN_STATES.get(run_id or "")
        if state is None:
            checkpoint = get_checkpoint(run_id)
            totals = (checkpoint or {}).get(
                "trade_totals",
                (checkpoint or {}).get("totals", {}),
            )
            top_volume = top_volume_rows(totals)
            if not top_volume:
                top_volume = get_persisted_top_volume(run_id)
            return {
                "status": "sin_datos",
                "logs": ["Spark | esperando eventos bursátiles."],
                "rows": [],
                "top_volume": top_volume,
                "activity_rows": (checkpoint or {}).get("activity_rows", []),
                "checkpoint": checkpoint,
            }
    query = state.get("query")
    persisted_checkpoint = get_checkpoint(run_id)
    if persisted_checkpoint is not None:
        persisted_processed_events = int(
            persisted_checkpoint.get("processed_events", 0)
        )
        if (
            persisted_processed_events > state["processed_events"]
            or not state["trade_totals"]
            or not state["activity_rows"]
        ):
            state["checkpoint"] = persisted_checkpoint
            state["input_events"] = int(
                persisted_checkpoint.get(
                    "input_events",
                    state["input_events"],
                )
            )
            state["batch_number"] = int(
                persisted_checkpoint.get(
                    "batch_number",
                    state["batch_number"],
                )
            )
            state["processed_events"] = persisted_processed_events
            state["processed_events_by_type"] = dict(
                persisted_checkpoint.get(
                    "processed_events_by_type",
                    state["processed_events_by_type"],
                )
            )
            state["trade_totals"] = dict(
                persisted_checkpoint.get(
                    "trade_totals",
                    state["trade_totals"],
                )
            )
            state["activity_rows"] = {
                f"{row['second']}|{row['event_type']}": row
                for row in persisted_checkpoint.get(
                    "activity_rows",
                    [],
                )
            }
    if query is not None:
        try:
            query_active = bool(query.isActive)
            query_error = None if query_active else query.exception()
            query_status = (
                "activa"
                if query_active
                else f"terminada:{query_error}"
            )
        except Exception as error:
            query_active = False
            query_status = f"error_consulta:{error}"
            query_error = error
        with _SPARK_LOCK:
            if query_status != state["query_status_signature"]:
                state["query_status_signature"] = query_status
                functions.append_console_log(
                    run_id,
                    "spark",
                    f"QUERY | run_id={run_id} | query_id={state['query_id']} | estado={query_status}",
                )
        if not query_active and query_error is not None:
            state["status"] = "error"
    monitor_workers(run_id, state)
    with _SPARK_LOCK:
        top_volume = top_volume_rows(state["trade_totals"])
        if not top_volume:
            top_volume = get_persisted_top_volume(run_id)
        return {
            "status": state["status"],
            "logs": list(state["logs"]),
            "rows": list(state["rows"]),
            "top_volume": top_volume,
            "activity_rows": list(state["activity_rows"].values()),
            "checkpoint": dict(state["checkpoint"]),
        }

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

def monitor_workers(run_id: str, state: dict[str, Any]) -> None:
    """Consulto jobs, stages y tasks reales del Driver y sus Workers."""

    try:
        def worker_number(host: str, worker_list: list[dict[str, Any]]) -> str | None:
            """Identifico el número del Worker a partir de sus datos reales."""

            for number in ("1", "2"):
                if host == f"spark-worker-{number}":
                    return number
            for worker in worker_list:
                identities = {
                    str(worker.get("host", "")),
                    str(worker.get("id", "")),
                }
                if host not in identities:
                    continue
                for identity in identities:
                    if identity.endswith("spark-worker-1"):
                        return "1"
                    if identity.endswith("spark-worker-2"):
                        return "2"
            return None

        applications = read_spark_api("/api/v1/applications")
        application = next(
            item for item in applications if item.get("name") == SPARK_APP_NAME
        )
        application_id = application["id"]
        workers = read_spark_api_from_master("/json/").get("workers", [])
        executors = read_spark_api(
            f"/api/v1/applications/{application_id}/executors"
        )
        query_started_at = state.get("query_started_at")
        jobs = [
            job
            for job in read_spark_api(
                f"/api/v1/applications/{application_id}/jobs"
            )
            if query_started_at is not None
            and _is_current_spark_item(job, query_started_at)
        ][:3]
        job_stage_ids = {
            str(stage_id)
            for job in jobs
            for stage_id in job.get("stageIds", [])
        }
        stages = [
            stage
            for stage in read_spark_api(
                f"/api/v1/applications/{application_id}/stages"
            )
            if str(stage.get("stageId")) in job_stage_ids
        ][:3]
        task_groups = []
        stage_workers: dict[str, set[str]] = {}
        for stage in stages:
            tasks = read_spark_api(
                f"/api/v1/applications/{application_id}/stages/"
                f"{stage['stageId']}/{stage['attemptId']}/taskList"
            )
            grouped_tasks: dict[tuple[str, str, str], int] = {}
            for task in tasks:
                host = str(task.get("host", "-"))
                worker = worker_number(host, workers)
                if worker is not None:
                    stage_workers.setdefault(
                        str(stage.get("stageId")),
                        set(),
                    ).add(worker)
                key = (
                    str(task.get("executorId", "-")),
                    worker or host,
                    str(task.get("status", "-")),
                )
                grouped_tasks[key] = grouped_tasks.get(key, 0) + 1
            task_groups.append(
                (stage.get("stageId"), stage.get("status"), stage.get("numTasks", 0), sorted(grouped_tasks.items()))
            )
        stable_workers = [
            {
                "coresused": worker.get("coresused"),
                "host": worker.get("host"),
                "id": worker.get("id"),
                "state": worker.get("state"),
            }
            for worker in workers
        ]
        stable_executors = [
            {
                "completedTasks": executor.get("completedTasks"),
                "failedTasks": executor.get("failedTasks"),
                "id": executor.get("id"),
                "isActive": executor.get("isActive"),
                "hostPort": executor.get("hostPort"),
                "totalTasks": executor.get("totalTasks"),
            }
            for executor in executors
        ]
        stable_jobs = [
            {
                "jobId": job.get("jobId"),
                "numCompletedTasks": job.get("numCompletedTasks"),
                "numFailedTasks": job.get("numFailedTasks"),
                "numTasks": job.get("numTasks"),
                "stageIds": job.get("stageIds"),
                "status": job.get("status"),
                "submissionTime": job.get("submissionTime"),
                "completionTime": job.get("completionTime"),
            }
            for job in jobs
        ]
        signature = json.dumps(
            [stable_workers, stable_executors, stable_jobs, task_groups],
            sort_keys=True,
            default=str,
        )
        if signature == state["monitor_signature"]:
            return
        state["monitor_signature"] = signature
        worker_names = ", ".join(
            f"Worker {index}: {worker.get('host')} | estado={worker.get('state')}"
            for index, worker in enumerate(workers, 1)
        )

        def worker_prefix(worker_labels: set[str]) -> str:
            """Genero el prefijo según la participación real observada."""

            if len(worker_labels) > 1:
                return "WORKERS"
            if worker_labels:
                return f"WORKER {next(iter(worker_labels))}"
            return "WORKER"

        monitor_lines = [
            f"WORKER | Workers activos | {worker_names or 'ninguno'}",
            f"WORKER | executors activos={len(executors)} | jobs observados={len(jobs)} | stages observados={len(stages)}",
        ]
        monitor_lines.extend(
            f"{worker_prefix({worker for stage_id in job.get('stageIds', []) for worker in stage_workers.get(str(stage_id), set())})} | job={job.get('jobId')} | estado={job.get('status')} | stages={job.get('stageIds')} | tasks={job.get('numTasks')} | completadas={job.get('numCompletedTasks')} | fallidas={job.get('numFailedTasks')} | inicio={job.get('submissionTime')} | fin={job.get('completionTime')}"
            + (
                f" | workers={','.join(sorted({worker for stage_id in job.get('stageIds', []) for worker in stage_workers.get(str(stage_id), set())}, key=int))}"
                if any(
                    stage_workers.get(str(stage_id))
                    for stage_id in job.get("stageIds", [])
                )
                else ""
            )
            for job in jobs
        )
        for stage_id, stage_status, task_count, groups in task_groups:
            workers_for_stage = stage_workers.get(str(stage_id), set())
            workers_text = (
                f" | workers={','.join(sorted(workers_for_stage, key=int))}"
                if workers_for_stage
                else ""
            )
            monitor_lines.append(
                f"{worker_prefix(workers_for_stage)} | stage={stage_id} | estado={stage_status} | tasks={task_count}{workers_text}"
            )
            monitor_lines.extend(
                f"{worker_prefix({host} if host in {'1', '2'} else set())} | stage={stage_id} | executor={executor_id} | worker={host} | estado={status} | tasks={count}"
                for (executor_id, host, status), count in groups
            )
        state["logs"] = (state["logs"] + monitor_lines)[-64:]
        for line in monitor_lines:
            functions.append_console_log(run_id, "spark", line)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, StopIteration):
        return

def _is_current_spark_item(
    item: dict[str, Any],
    query_started_at: datetime,
) -> bool:
    """Determino si un Job o Stage comenzó con la query actual."""

    submission_time = item.get("submissionTime")
    if not submission_time:
        return False
    try:
        submitted_at = datetime.fromisoformat(
            str(submission_time).replace("GMT", "+00:00")
        )
    except ValueError:
        return False
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=timezone.utc)
    return submitted_at >= query_started_at

def read_spark_api(path: str) -> Any:
    """Leo un endpoint JSON del Spark Driver."""

    with urllib.request.urlopen(f"{SPARK_UI_URL}{path}", timeout=2) as response:
        return json.load(response)

def read_spark_api_from_master(path: str) -> Any:
    """Leo un endpoint JSON del Spark Master."""

    with urllib.request.urlopen(
        f"{SPARK_MASTER_UI_URL}{path}",
        timeout=2,
    ) as response:
        return json.load(response)

def process_batch(run_id: str, data_frame: DataFrame, batch_id: int) -> None:
    """Proceso un microbatch distribuido y persisto sus resultados."""

    input_frame = data_frame.persist()
    processed_frame = None
    try:
        input_count = input_frame.count()
        if not input_count:
            return
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
        parquet_path = str(
            DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_data.parquet.parts"
        )
        processed_frame.write.mode("append").parquet(parquet_path)
        grouped = processed_frame.groupBy("symbol").agg(
            count(when(col("event_type") == "TRADE", True)).alias("operations"),
            count(when(col("event_type") == "QUOTE", True)).alias("quotes"),
            sum_rows(when(col("event_type") == "TRADE", col("size"))).alias("total_volume"),
        ).orderBy("symbol")
        result_rows = [row.asDict() for row in grouped.collect()]
        activity_frame = processed_frame.withColumn(
            "second",
            date_trunc("second", regexp_replace(
                col("timestamp"), r"(\.\d{6})\d+(Z)$", r"$1$2"
            ).cast("timestamp")),
        ).where(col("second").isNotNull())
        activity_rows = [
            {"event_type": row["event_type"], "events": int(row["events"]),
             "second": row["second"].isoformat()}
            for row in activity_frame.groupBy("second", "event_type")
            .agg(count("*").alias("events"))
            .orderBy("second", "event_type").collect()
        ]
        with _SPARK_LOCK:
            state = _RUN_STATES[run_id]
            state["input_events"] += input_count
            for row in result_rows:
                symbol = str(row["symbol"])
                state["trade_totals"][symbol] = (
                    state["trade_totals"].get(symbol, 0.0)
                    + float(row["total_volume"] or 0)
                )
            state["processed_events"] += processed_count
            for event_type, field in (("TRADE", "operations"), ("QUOTE", "quotes")):
                state["processed_events_by_type"][event_type] += sum(
                    int(row[field] or 0) for row in result_rows
                )
            state["batch_number"] += 1
            state["rows"] = result_rows
            for row in activity_rows:
                key = f"{row['second']}|{row['event_type']}"
                previous = state["activity_rows"].get(key, {}).get("events", 0)
                state["activity_rows"][key] = {**row, "events": previous + row["events"]}
            state["status"] = "completado"
            state["checkpoint"] = {
                "input_events": state["input_events"],
                "run_id": run_id,
                "processed_events": state["processed_events"],
                "batch_number": state["batch_number"],
                "trade_totals": dict(state["trade_totals"]),
                "processed_events_by_type": dict(state["processed_events_by_type"]),
                "activity_rows": list(state["activity_rows"].values()),
            }
            checkpoint = dict(state["checkpoint"])
            totals = dict(state["trade_totals"])
        # Conservamos el registro canónico usado para recuperar gráficos después de F5.
        info_path = DATA_DIRECTORY / "runs" / f"{run_id}_00_run_info.jsonl"
        with info_path.open("a", encoding="utf-8") as info_file:
            info_file.write(json.dumps(
                {"record_type": "spark_checkpoint", **checkpoint}, ensure_ascii=False
            ) + "\n")
            info_file.flush()
            os.fsync(info_file.fileno())
        for message in (
            f"DATAFRAME | batch_id={batch_id} | registros_entrada={input_count} | validos={valid_count} | invalidos={input_count - valid_count} | duplicados_eliminados={valid_count - processed_count}",
            f"PARQUET | batch_id={batch_id} | ruta={parquet_path} | registros={processed_count}",
            f"EDA | batch_id={batch_id} | simbolos={len(result_rows)} | operaciones={checkpoint['processed_events_by_type']['TRADE']} | cotizaciones={checkpoint['processed_events_by_type']['QUOTE']}",
            f"EDA | batch_id={batch_id} | volumen_trade_por_accion={top_volume_rows(totals)}",
        ):
            functions.append_console_log(run_id, "spark", message)
    except Exception as error:
        functions.append_console_log(
            run_id, "spark", f"ERROR | batch_id={batch_id} | procesamiento={error}"
        )
        raise
    finally:
        input_frame.unpersist()
        if processed_frame is not None:
            processed_frame.unpersist()

def consolidate_parquet(run_id: str) -> bool:
    """Consolido los archivos Parquet temporales en un único archivo final."""

    parts_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_data.parquet.parts"
    final_path = DATA_DIRECTORY / "runs" / f"{run_id}_04_spark_data.parquet"
    temporary_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_data.parquet.tmp"
    checkpoint_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_checkpoint"
    if final_path.exists():
        return True
    if not parts_path.exists():
        return False
    if temporary_path.exists():
        raise FileExistsError(
            f"Existe una consolidación temporal pendiente para {run_id}."
        )
    spark = get_spark_session()
    if spark is None:
        raise RuntimeError("Spark no está disponible para consolidar Parquet.")
    processed_frame = spark.read.parquet(str(parts_path))
    processed_count = processed_frame.count()
    if processed_count == 0:
        return False
    processed_frame.coalesce(1).write.mode("overwrite").parquet(
        str(temporary_path)
    )
    part_files = sorted(temporary_path.glob("part-*.parquet"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"La consolidación de {run_id} generó {len(part_files)} archivos."
        )
    (DATA_DIRECTORY / "runs").mkdir(parents=True, exist_ok=True)
    temporary_file = DATA_DIRECTORY / "runs" / f".{final_path.name}.tmp"
    shutil.copy2(part_files[0], temporary_file)
    os.replace(temporary_file, final_path)
    verified_count = spark.read.parquet(str(final_path)).count()
    if verified_count != processed_count:
        raise RuntimeError(
            f"La verificación de {run_id} esperaba {processed_count} "
            f"registros y obtuvo {verified_count}."
        )
    shutil.rmtree(temporary_path)
    shutil.rmtree(parts_path)
    shutil.rmtree(checkpoint_path, ignore_errors=True)
    return True

def start_streaming(run_id: str, mode: str = "REPLAY") -> None:
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
            "trade_totals": {},
            "processed_events_by_type": {"TRADE": 0, "QUOTE": 0},
            "activity_rows": [],
        }
        _RUN_STATES[run_id] = {
            "batch_number": int(checkpoint.get("batch_number", 0)),
            "checkpoint": checkpoint,
            "logs": [],
            "monitor_signature": "",
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
            "query_started_at": None,
            "query_status_signature": "",
            "rows": [],
            "status": "iniciando",
            "activity_rows": {
                f"{row['second']}|{row['event_type']}": row
                for row in checkpoint.get("activity_rows", [])
            },
            "trade_totals": dict(checkpoint.get("trade_totals", {})),
        }
    spark = get_spark_session()
    if spark is None:
        error_message = _SPARK_ERROR or "Spark no disponible."
        with _SPARK_LOCK:
            _STARTING_RUNS.discard(run_id)
            _RUN_STATES[run_id]["status"] = "error"
            _RUN_STATES[run_id]["logs"] = [error_message]
        functions.append_console_log(run_id, "spark", error_message)
        raise RuntimeError(error_message)
    (DATA_DIRECTORY / "spark").mkdir(parents=True, exist_ok=True)
    checkpoint_path = str(DATA_DIRECTORY / "spark" / f"{run_id}_04_spark_checkpoint")
    query_started_at = datetime.now(timezone.utc)
    try:
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
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
        _RUN_STATES[run_id]["query_started_at"] = query_started_at
        _RUN_STATES[run_id]["query_status_signature"] = "activa"
    functions.append_console_log(
        run_id,
        "spark",
        f"QUERY | run_id={run_id} | query_id={query.id} | estado=iniciada",
    )

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
        query = state.get("query") if state else None
    if query is not None:
        try:
            if query.isActive:
                query.stop()
        except Exception as error:
            functions.append_console_log(run_id, "spark", f"ERROR | STOP | cierre Spark: {error}")
            if not has_active_query():
                reset_spark_runtime()
            with _SPARK_LOCK:
                _RUN_STATES.pop(run_id, None)
            return False
    if finalize:
        try:
            consolidated = consolidate_parquet(run_id)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "spark",
                f"ERROR | consolidación Parquet: {error}",
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

def top_volume_rows(totals: dict[str, float]) -> list[dict[str, Any]]:
    """Ordeno el ranking acumulado por volumen exclusivamente TRADE."""

    return [
        {"symbol": symbol, "total_volume": volume}
        for symbol, volume in sorted(
            totals.items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]
    ]
