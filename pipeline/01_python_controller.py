# Autor: Nicolás Druetta.

import atexit
import builtins
import json
import os
import sys
import threading
import uuid

from app import functions
from functools import partial
from typing import Any

_ACTIVE_SERVICE: Any = None
_ACTIVE_SERVICE_REGISTRY_KEY = "_IAPD_ACTIVE_SERVICE"
_FAILED_RUNS: set[str] = set()
_HEARTBEAT_SESSION_ID = f"st-{uuid.uuid4().hex[:8]}"
_KAFKA_MODULE: Any = None
_KAFKA_PREPARED_RUNS: set[str] = set()
_LAST_STOPPED_SNAPSHOT: dict[str, Any] | None = None
_MANAGER_LOCK = threading.RLock()
_RECOVERY_THREAD: threading.Thread | None = None
_RUN_LOG_COUNTS: dict[str, int] = {}
_RUN_SEQUENCES: dict[str, int] = {}
_SPARK_MODULE: Any = None
_SPARK_PENDING_RUNS: set[str] = set()
_SPARK_STARTED_RUNS: set[str] = set()
_STAGE_NOTIFICATIONS: set[tuple[str, str]] = set()
_STREAMING_MODULE: Any = None
HEARTBEAT_TIMEOUT_MINUTES = 15

def append_run_record(
    run_id: str,
    record_type: str,
    payload: dict[str, Any],
) -> None:
    """Agrego un registro general al JSONL persistente del RUN."""

    run_info_path = DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_info.jsonl"
    with _MANAGER_LOCK:
        sequence = _RUN_SEQUENCES.get(run_id)
        if sequence is None:
            streaming_module = _require_streaming_module()
            state = streaming_module.read_streaming_state(run_info_path)
            sequence = int(state.get("sequence", 0)) if state else 0
        sequence += 1
        _RUN_SEQUENCES[run_id] = sequence
        record = {
            "record_type": record_type,
            "sequence": sequence,
            "run_id": run_id,
            "recorded_at": _require_streaming_module().get_utc_timestamp(),
            **payload,
        }
        if record_type == "run_started":
            functions.append_console_log(
                run_id, "controller", "Python | RUN registrado"
            )
            record.update(
                {
                    "info_controller_file": f"{run_id}_01_python_controller_info.jsonl",
                    "log_controller_file": f"{run_id}_01_python_controller_log.txt",
                    "log_alpaca_file": f"{run_id}_02_alpaca_stream_log.txt",
                    "log_spark_file": f"{run_id}_04_apache_spark_log.txt",
                    "log_kafka_file": f"{run_id}_03_apache_kafka_log.txt",
                    "log_pandas_file": f"{run_id}_05_pandas_log.txt",
                    "log_postgresql_file": f"{run_id}_06_postgresql_log.txt",
                }
            )
        if record_type == "run_finished":
            functions.append_console_log(
                run_id,
                "controller",
                f"Python | RUN terminado | estado={payload.get('status')} | causa={payload.get('end_reason')}",
            )
        run_info_path.parent.mkdir(parents=True, exist_ok=True)
        with run_info_path.open("a", encoding="utf-8") as info_controller_file:
            info_controller_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            info_controller_file.flush()
            os.fsync(info_controller_file.fileno())

def append_run_state(service: Any) -> None:
    """Persisto el estado actual del RUN administrado por el Controller."""

    snapshot = service.get_snapshot()
    log_count = _RUN_LOG_COUNTS.get(snapshot["run_id"], 0)
    append_run_record(
        snapshot["run_id"],
        "state",
        {
            "downloaded_event_count": snapshot["downloaded_event_count"],
            "ended_at": snapshot["ended_at"],
            "end_reason": snapshot["end_reason"],
            "error": snapshot["error"],
            "event_count": snapshot["event_count"],
            "historical_date": snapshot["historical_date"],
            "is_active": snapshot["is_active"],
            "last_heartbeat": snapshot["last_heartbeat"],
            "log_count": log_count,
            "market_timestamp": snapshot["market_timestamp"],
            "replay_position": snapshot["replay_position"],
            "started_at": snapshot["started_at"],
            "status": snapshot["status"],
        },
    )

def _cancel_pipeline(run_id: str, reason: str) -> None:
    """Cancelo las etapas del RUN sin detener los servicios compartidos."""

    if _SPARK_MODULE is not None:
        try:
            _SPARK_MODULE.stop_streaming(run_id, finalize=False)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "controller",
                f"Python | error al detener Spark durante la cancelación | {error}",
            )
    if _KAFKA_MODULE is not None:
        _KAFKA_MODULE.reset_producer()
    functions.append_console_log(
        run_id,
        "controller",
        f"Python | cancelando RUN | motivo={reason}",
    )

def _release_run_resources(run_id: str) -> None:
    """Libero Spark y Kafka del RUN sin reemplazar el resultado original."""

    if _SPARK_MODULE is not None:
        try:
            _SPARK_MODULE.stop_streaming(run_id, finalize=False)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "controller",
                f"Python | error al liberar Spark | {error}",
            )
    if _KAFKA_MODULE is not None:
        try:
            _KAFKA_MODULE.reset_producer()
        except Exception as error:
            functions.append_console_log(
                run_id,
                "controller",
                f"Python | error al liberar Kafka | {error}",
            )

def fail_run(run_id: str, error: str, stage: str = "Controller") -> None:
    """Cancelo globalmente un RUN ante un error fatal de cualquier etapa."""

    with _MANAGER_LOCK:
        if run_id in _FAILED_RUNS:
            return
        _FAILED_RUNS.add(run_id)
    service = get_active_service()
    if service is not None and service.get_snapshot().get("run_id") == run_id:
        functions.append_console_log(
            run_id,
            "controller",
            f"Python | error fatal en {stage} | {error}",
        )
        service.fail(error, f"error_{stage.lower()}")
        return
    functions.append_console_log(
        run_id,
        "controller",
        f"Python | error fatal en {stage} | {error}",
    )
    append_run_record(
        run_id,
        "run_finished",
        {
            "status": "Error",
            "end_reason": f"error_{stage.lower()}",
            "error": error,
            "ended_at": _require_streaming_module().get_utc_timestamp(),
        },
    )
    _cancel_pipeline(run_id, f"error_{stage.lower()}")

def fail_active_run(error: str) -> None:
    """Finalizo con error el RUN activo cuando falla su inicialización."""

    service = get_active_service()
    if service is not None:
        fail_run(service.get_snapshot()["run_id"], error)

def get_active_service() -> Any:
    """Devuelvo el servicio Alpaca administrado por el controlador."""

    with _MANAGER_LOCK:
        return getattr(builtins, _ACTIVE_SERVICE_REGISTRY_KEY, _ACTIVE_SERVICE)

def get_run_snapshot() -> dict[str, Any]:
    """Obtengo el estado activo o la última ejecución persistida."""

    streaming_module = _require_streaming_module()
    service = get_active_service()
    if service is not None:
        snapshot = service.get_snapshot()
        run_id = snapshot["run_id"]
        snapshot["log_count"] = _RUN_LOG_COUNTS.get(run_id, 0)
        snapshot.update(
            {
                "info_controller_file": f"{run_id}_01_python_controller_info.jsonl",
                "log_controller_file": f"{run_id}_01_python_controller_log.txt",
                "log_alpaca_file": f"{run_id}_02_alpaca_stream_log.txt",
                "log_spark_file": f"{run_id}_04_apache_spark_log.txt",
                "log_kafka_file": f"{run_id}_03_apache_kafka_log.txt",
                "log_pandas_file": f"{run_id}_05_pandas_log.txt",
                "log_postgresql_file": f"{run_id}_06_postgresql_log.txt",
            }
        )
        return snapshot
    if _LAST_STOPPED_SNAPSHOT is not None:
        return dict(_LAST_STOPPED_SNAPSHOT)
    metadata = streaming_module.get_latest_metadata()
    if metadata is not None:
        metadata.setdefault("downloaded_event_count", 0)
        metadata.setdefault("historical_date", None)
        metadata.setdefault("log_count", 0)
        metadata.setdefault("logs", [])
        metadata.setdefault("replay_position", 0)
        metadata.setdefault("replay_speed", 1.0)
        metadata.setdefault("universe", {"selection": "manual"})
        return metadata
    return {
        "downloaded_event_count": 0,
        "ended_at": None,
        "end_reason": None,
        "error": None,
        "event_count": 0,
        "historical_date": None,
        "is_active": False,
        "last_heartbeat": None,
        "log_count": 0,
        "logs": [],
        "replay_position": 0,
        "replay_speed": 1.0,
        "run_id": None,
        "started_at": None,
        "status": "Listo para ejecutar",
        "symbols": [],
        "universe": {"selection": "manual", "symbols": []},
    }

def mark_interrupted_run() -> dict[str, Any] | None:
    """Reconozco un RUN activo que pertenecía a otro proceso Python."""

    streaming_module = _require_streaming_module()
    metadata = streaming_module.get_latest_metadata()
    if metadata is None or not metadata.get("is_active"):
        return metadata
    end_reason = metadata.get("end_reason") or "process_restart"
    metadata["status"] = "Incompleto"
    metadata["is_active"] = False
    metadata["end_reason"] = end_reason
    metadata["ended_at"] = streaming_module.get_utc_timestamp()
    run_id = metadata.get("run_id")
    if isinstance(run_id, str) and isinstance(metadata.get("info_controller_file"), str):
        sequence = int(metadata.get("sequence", 0))
        _RUN_SEQUENCES[run_id] = sequence
        append_run_record(
            run_id,
            "state",
            {
                "status": "Incompleto",
                "is_active": False,
                "end_reason": end_reason,
                "ended_at": metadata["ended_at"],
            },
        )
    return metadata

def register_heartbeat(run_id: str) -> bool:
    """Valido y registro el heartbeat de una sesión visual activa."""

    service = get_active_service()
    return (
        service.heartbeat(run_id, _HEARTBEAT_SESSION_ID)
        if service is not None
        else False
    )

def get_heartbeat_session_id() -> str:
    """Devuelvo el identificador de esta instancia del controlador."""

    return _HEARTBEAT_SESSION_ID

def start_pipeline(run_id: str) -> None:
    """Preparo Kafka y libero Alpaca para publicar la primera ventana."""

    if _KAFKA_MODULE is None or _SPARK_MODULE is None:
        raise RuntimeError("Kafka y Spark no fueron configurados.")
    with _MANAGER_LOCK:
        if run_id in _KAFKA_PREPARED_RUNS:
            return
    functions.append_console_log(run_id, "controller", "Python | iniciando Kafka")
    try:
        _KAFKA_MODULE.ensure_topic()
        _KAFKA_MODULE.get_producer()
    except Exception as error:
        functions.append_console_log(
            run_id,
            "controller",
            f"Python | ERROR Kafka | causa={error}",
        )
        raise
    functions.append_console_log(
        run_id,
        "controller",
        "Python | Kafka iniciado correctamente",
    )
    if release_active_pipeline(run_id):
        with _MANAGER_LOCK:
            _KAFKA_PREPARED_RUNS.add(run_id)

def _notify_first_publication(run_id: str) -> None:
    """Registro la primera publicación y solicito el inicio desacoplado de Spark."""

    with _MANAGER_LOCK:
        if (
            run_id in _SPARK_PENDING_RUNS
            or run_id in _SPARK_STARTED_RUNS
        ):
            return
        _SPARK_PENDING_RUNS.add(run_id)
    functions.append_console_log(
        run_id,
        "controller",
        "Python | primera publicación Kafka confirmada",
    )

def _start_pending_spark(run_id: str) -> None:
    """Inicio Spark desde el supervisor, fuera del callback de Kafka."""

    with _MANAGER_LOCK:
        if run_id not in _SPARK_PENDING_RUNS:
            return
        _SPARK_PENDING_RUNS.discard(run_id)
        if run_id in _SPARK_STARTED_RUNS:
            return
        _SPARK_STARTED_RUNS.add(run_id)
    try:
        functions.append_console_log(run_id, "controller", "Python | iniciando Spark")
        _SPARK_MODULE.start_streaming(run_id)
        if not _SPARK_MODULE.has_active_query():
            raise RuntimeError("Structured Streaming no quedó activo.")
        _notify_stage_status(run_id, "Spark_success")
    except Exception as error:
        with _MANAGER_LOCK:
            _SPARK_STARTED_RUNS.discard(run_id)
        functions.append_console_log(
            run_id,
            "controller",
            f"Python | ERROR Spark | causa={error}",
        )
        fail_active_run(str(error))

def _notify_stage_status(
    run_id: str,
    stage: str,
    error: str | None = None,
) -> None:
    """Registro una sola confirmación real de una etapa por RUN."""

    notification = (run_id, stage)
    with _MANAGER_LOCK:
        if notification in _STAGE_NOTIFICATIONS:
            return
        _STAGE_NOTIFICATIONS.add(notification)
    messages = {
        "Pandas_error": "Python | ERROR Pandas | causa={error}",
        "Pandas_start": "Python | iniciando Pandas",
        "Pandas_success": "Python | Pandas iniciado correctamente",
        "PostgreSQL_error": "Python | ERROR PostgreSQL | causa={error}",
        "PostgreSQL_start": "Python | iniciando PostgreSQL",
        "PostgreSQL_success": "Python | PostgreSQL iniciado correctamente",
        "Spark_success": "Python | Spark iniciado correctamente",
    }
    message = messages.get(stage)
    if message is not None:
        functions.append_console_log(
            run_id,
            "controller",
            message.format(error=error or "causa no especificada"),
        )

def _coordinate_written_batch(
    run_id: str,
    batch_id: int,
    pandas_module: Any,
    postgresql_module: Any,
) -> bool:
    """Coordino Pandas y PostgreSQL después de Parquet confirmado."""

    try:
        if pandas_module is None:
            raise RuntimeError("Pandas no está configurado en el Controller.")
        analysis = pandas_module.analyze(run_id, batch_id=batch_id)
        if analysis is None:
            raise RuntimeError(
                f"Pandas no produjo resultados para batch_id={batch_id}."
            )
    except Exception as error:
        fail_run(run_id, str(error), "Pandas")
        return False
    try:
        if postgresql_module is None:
            raise RuntimeError(
                "PostgreSQL no está configurado en el Controller."
            )
        if not postgresql_module.persist_analysis(run_id, analysis):
            raise RuntimeError(
                f"PostgreSQL no confirmó batch_id={batch_id}."
            )
    except Exception as error:
        fail_run(run_id, str(error), "PostgreSQL")
        return False
    return True

def release_active_pipeline(run_id: str) -> bool:
    """Habilito el primer lote cuando las consultas del RUN están preparadas."""

    service = get_active_service()
    if service is None or service.get_snapshot()["run_id"] != run_id:
        return False
    service.release_pipeline()
    return True

def release_active_service(service: Any) -> None:
    """Libero el RUN terminado para permitir una nueva ejecución."""

    global _ACTIVE_SERVICE
    with _MANAGER_LOCK:
        snapshot = service.get_snapshot()
        run_id = snapshot.get("run_id")
        registered_service = getattr(
            builtins,
            _ACTIVE_SERVICE_REGISTRY_KEY,
            _ACTIVE_SERVICE,
        )
        if (
            not isinstance(run_id, str)
            or (_ACTIVE_SERVICE is not service and registered_service is not service)
        ):
            return
        if _ACTIVE_SERVICE is service:
            _ACTIVE_SERVICE = None
        if getattr(builtins, _ACTIVE_SERVICE_REGISTRY_KEY, None) is service:
            delattr(builtins, _ACTIVE_SERVICE_REGISTRY_KEY)
        _KAFKA_PREPARED_RUNS.discard(run_id)
        _SPARK_PENDING_RUNS.discard(run_id)
        _SPARK_STARTED_RUNS.discard(run_id)
    _release_run_resources(run_id)
    if snapshot.get("status") == "Error":
        functions.append_console_log(
            run_id,
            "controller",
            f"Python | cancelando RUN | motivo={snapshot.get('end_reason') or 'error_pipeline'}",
        )

def request_stop_active_run(end_reason: str) -> None:
    """Solicito la detención inmediata del servicio activo."""

    service = get_active_service()
    if service is not None:
        snapshot = service.get_snapshot()
        if snapshot.get("end_reason") is None:
            functions.append_console_log(
                snapshot["run_id"],
                "controller",
                f"Python | detención solicitada | causa={end_reason}",
            )
        service.request_stop(end_reason)
        return
    if end_reason != "manual_stop":
        return
    streaming_module = _require_streaming_module()
    metadata = streaming_module.get_latest_metadata()
    run_id = metadata.get("run_id") if metadata is not None else None
    if (
        metadata is None
        or not metadata.get("is_active")
        or not isinstance(run_id, str)
    ):
        return
    functions.append_console_log(
        run_id, "controller", "Python | detención manual solicitada"
    )
    append_run_record(
        run_id,
        "state",
        {
            "status": "Finalizando",
            "is_active": True,
            "end_reason": "manual_stop",
        },
    )

def configure_streaming_module(streaming_module: Any) -> None:
    """Registro el módulo Alpaca que utiliza el controlador."""

    global _STREAMING_MODULE
    if _STREAMING_MODULE is streaming_module:
        return
    active_service = get_active_service()
    _STREAMING_MODULE = streaming_module
    if active_service is None:
        mark_interrupted_run()

def _consolidate_recovered_run(
    spark_module: Any,
    metadata: dict[str, Any],
    run_id: str,
) -> None:
    """Consolido en segundo plano un RUN detenido antes del reinicio."""

    try:
        consolidated = spark_module.consolidate_parquet(run_id)
    except Exception:
        try:
            if not spark_module.has_active_query():
                spark_module.reset_spark_runtime()
            consolidated = spark_module.consolidate_parquet(run_id)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "spark",
                f"ERROR | consolidación Parquet después del reinicio: {error}",
            )
            return
    if not consolidated:
        return
    ended_at = metadata.get("ended_at") or _STREAMING_MODULE.get_utc_timestamp()
    end_reason = metadata.get("end_reason") or "process_restart"
    append_run_record(
        run_id,
        "run_finished",
        {
            "status": "Detenido",
            "end_reason": end_reason,
            "error": None,
            "ended_at": ended_at,
        },
    )
    append_run_record(
        run_id,
        "state",
        {
            "status": "Detenido",
            "is_active": False,
            "end_reason": end_reason,
            "ended_at": ended_at,
        },
    )

def configure_pipeline_modules(
    kafka_module: Any,
    spark_module: Any,
) -> None:
    """Registro Kafka y Spark para coordinar el flujo del RUN."""

    global _KAFKA_MODULE, _SPARK_MODULE, _RECOVERY_THREAD
    _KAFKA_MODULE = kafka_module
    _SPARK_MODULE = spark_module
    spark_module.configure_failure_callback(
        lambda run_id, error: fail_run(run_id, error, "Spark")
    )
    pandas_module = sys.modules.get("05_pandas")
    postgresql_module = sys.modules.get("06_postgresql")
    if pandas_module is not None:
        pandas_module.configure_status_callback(_notify_stage_status)
    if postgresql_module is not None:
        postgresql_module.configure_status_callback(_notify_stage_status)
    spark_module.configure_batch_written_callback(
        lambda batch_run_id, batch_id: _coordinate_written_batch(
            batch_run_id,
            batch_id,
            pandas_module,
            postgresql_module,
        )
    )
    metadata = _STREAMING_MODULE.get_latest_metadata()
    if (
        metadata is None
        or metadata.get("status") not in {"Detenido", "Incompleto"}
        or metadata.get("is_active")
        or not isinstance(metadata.get("run_id"), str)
    ):
        return
    run_id = metadata["run_id"]
    parts_path = DATA_DIRECTORY / "spark" / f"{run_id}_04_apache_spark_data.parquet.parts"
    status_path = DATA_DIRECTORY / "runs" / f"{run_id}_04_apache_spark_data_status.txt"
    if not parts_path.exists() and not status_path.exists():
        return
    if _RECOVERY_THREAD is not None and _RECOVERY_THREAD.is_alive():
        return
    _RECOVERY_THREAD = threading.Thread(
        target=_consolidate_recovered_run,
        args=(spark_module, metadata, run_id),
        daemon=True,
        name=f"parquet-recovery-{run_id}",
    )
    _RECOVERY_THREAD.start()

def _manual_stop_was_requested(metadata: dict[str, Any] | None) -> bool:
    """Detecto un STOP manual aunque otro hilo haya escrito después."""

    if metadata is None or not isinstance(metadata.get("run_id"), str):
        return False
    run_info_path = DATA_DIRECTORY / "runs" / (
        f"{metadata['run_id']}_01_python_controller_info.jsonl"
    )
    try:
        with run_info_path.open(encoding="utf-8") as info_file:
            return any(
                json.loads(line).get("end_reason") == "manual_stop"
                for line in info_file
                if line.strip()
            )
    except (OSError, json.JSONDecodeError):
        return False

def shutdown_active_run() -> None:
    """Cierro el RUN administrado cuando finaliza el proceso Python."""

    service = get_active_service()
    metadata = (
        _STREAMING_MODULE.get_latest_metadata()
        if _STREAMING_MODULE is not None
        else None
    )
    end_reason = (
        "manual_stop"
        if (
            service is not None
            and service.get_snapshot().get("end_reason") == "manual_stop"
        ) or _manual_stop_was_requested(metadata)
        else "process_shutdown"
    )
    stop_active_run(end_reason)

def start_run(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str = "iex",
    ranking: str | None = None,
    replay_speed: float = 1.0,
) -> Any:
    """Creo un único RUN y coordino su ciclo de vida."""

    global _ACTIVE_SERVICE, _LAST_STOPPED_SNAPSHOT
    recovery_thread = _RECOVERY_THREAD
    if recovery_thread is not None and recovery_thread.is_alive():
        recovery_thread.join()
    streaming_module = _require_streaming_module()
    if _KAFKA_MODULE is None or _SPARK_MODULE is None:
        raise RuntimeError("Kafka y Spark no fueron configurados.")
    with _MANAGER_LOCK:
        active_service = get_active_service()
        if active_service is not None and active_service.is_active():
            _ACTIVE_SERVICE = active_service
            return active_service
        functions.ensure_pipeline_directories()
        started_at = streaming_module.get_utc_timestamp()
        run_id = streaming_module.build_run_stem(started_at)
        run_info_path = DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_info.jsonl"
        if run_info_path.exists():
            raise RuntimeError(
                "El identificador del RUN ya existe; esperá un segundo y volvé a iniciar."
            )
        functions.append_console_log(
            run_id,
            "controller",
            f"Python | iniciando ejecución | run_id={run_id}",
        )
        _LAST_STOPPED_SNAPSHOT = None
        try:
            functions.append_console_log(
                run_id,
                "controller",
                "Python | iniciando Alpaca",
            )
            _ACTIVE_SERVICE = streaming_module.AlpacaStreamingService(
                api_key,
                api_secret,
                run_id,
                symbols,
                started_at,
                run_id,
                None,
                feed,
                replay_speed,
                ranking=ranking,
                release_callback=release_active_service,
                publish_events_callback=partial(
                    _KAFKA_MODULE.publish_events,
                    first_publication_callback=_notify_first_publication,
                ),
                append_run_record_callback=append_run_record,
                append_run_state_callback=append_run_state,
            )
        except FileExistsError as error:
            raise RuntimeError(
                "El identificador del RUN ya existe; esperá un segundo y volvé a iniciar."
            ) from error
        setattr(builtins, _ACTIVE_SERVICE_REGISTRY_KEY, _ACTIVE_SERVICE)
        _ACTIVE_SERVICE.start()
        functions.append_console_log(
            run_id,
            "controller",
            "Python | Alpaca iniciado correctamente",
        )
        supervisor_thread = threading.Thread(
            target=_supervise_heartbeat,
            args=(_ACTIVE_SERVICE,),
            daemon=True,
            name=f"controller-supervisor-{run_id}",
        )
        supervisor_thread.start()
        _KAFKA_MODULE.initialize_run(run_id)
        return _ACTIVE_SERVICE

def stop_active_run(end_reason: str) -> None:
    """Detengo el único RUN administrado por el controlador."""

    global _LAST_STOPPED_SNAPSHOT
    service = get_active_service()
    try:
        if service is not None:
            functions.append_console_log(
                service.get_snapshot()["run_id"],
                "controller",
                "Python | finalizando ejecución",
            )
            service.stop(end_reason)
            _LAST_STOPPED_SNAPSHOT = service.get_snapshot()
            functions.append_console_log(
                service.get_snapshot()["run_id"],
                "controller",
                "Python | ejecución finalizada",
            )
    finally:
        if service is not None:
            release_active_service(service)
        if _KAFKA_MODULE is not None:
            _KAFKA_MODULE.reset_producer()

def _require_streaming_module() -> Any:
    """Obtengo el módulo Alpaca configurado para el controlador."""

    if _STREAMING_MODULE is None:
        raise RuntimeError("El módulo Alpaca no fue configurado.")
    return _STREAMING_MODULE

def _supervise_heartbeat(service: Any) -> None:
    """Cancelo el RUN tras 15 minutos sin señales del navegador."""

    run_id = service.get_snapshot()["run_id"]
    while service.is_active():
        if service.get_snapshot()["end_reason"] is not None:
            return
        _start_pending_spark(run_id)
        if service.heartbeat_elapsed() >= HEARTBEAT_TIMEOUT_MINUTES * 60:
            # Detengo la fuente primero; después cancelo Spark para este RUN.
            if get_active_service() is not service:
                return
            if service.heartbeat_elapsed() < HEARTBEAT_TIMEOUT_MINUTES * 60:
                continue
            functions.append_console_log(
                run_id,
                "controller",
                "Python | tiempo límite sin heartbeat | solicitando detención",
            )
            service.request_stop("heartbeat_timeout")
            try:
                stop_active_run("heartbeat_timeout")
            finally:
                if _SPARK_MODULE is not None:
                    _SPARK_MODULE.stop_streaming(run_id, finalize=False)
            return
        threading.Event().wait(1)

atexit.register(shutdown_active_run)
