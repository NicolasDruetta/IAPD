import atexit
import functions
import json
import os
import threading
import time

from datetime import timezone
from typing import Any

_ACTIVE_SERVICE: Any = None
_KAFKA_MODULE: Any = None
_MANAGER_LOCK = threading.RLock()
_RUN_LOG_COUNTS: dict[str, int] = {}
_RUN_SEQUENCES: dict[str, int] = {}
_SPARK_MODULE: Any = None
_STREAMING_MODULE: Any = None
_SUPERVISOR_THREAD: threading.Thread | None = None
HEARTBEAT_TIMEOUT_SECONDS = 900

def append_controller_log(
    run_id: str,
    message: str,
    service: Any,
) -> None:
    """Registro un mensaje y persisto el estado administrado del RUN."""

    streaming_module = _require_streaming_module()
    timestamp = streaming_module.format_presentation_timestamp(
        streaming_module.get_utc_timestamp(),
        source_timezone=timezone.utc,
    )
    functions.append_console_log(
        run_id,
        "controller",
        f"{timestamp} | {message}",
    )
    with _MANAGER_LOCK:
        _RUN_LOG_COUNTS[run_id] = _RUN_LOG_COUNTS.get(run_id, 0) + 1
    append_run_state(service)

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
            record.update(
                {
                    "run_info_file": f"{run_id}_01_python_controller_info.jsonl",
                    "log_controller_file": f"{run_id}_01_python_controller_log.txt",
                    "log_alpaca_file": f"{run_id}_02_alpaca_stream_log.txt",
                    "log_spark_file": f"{run_id}_04_apache_spark_log.txt",
                    "kafka_log_file": f"{run_id}_03_apache_kafka_log.txt",
                    "log_pandas_postgresql_file": f"{run_id}_05_pandas_postgresql_log.txt",
                }
            )
        run_info_path.parent.mkdir(parents=True, exist_ok=True)
        with run_info_path.open("a", encoding="utf-8") as run_info_file:
            run_info_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            run_info_file.flush()
            os.fsync(run_info_file.fileno())

def append_run_state(service: Any) -> None:
    """Persisto el estado actual del RUN administrado por el Controller."""

    snapshot = service.get_snapshot()
    log_count = _RUN_LOG_COUNTS.get(snapshot["run_id"], 0)
    append_run_record(
        snapshot["run_id"],
        "state",
        {
            "status": snapshot["status"],
            "error": snapshot["error"],
            "event_count": snapshot["event_count"],
            "log_count": log_count,
            "market_timestamp": snapshot["market_timestamp"],
            "replay_position": snapshot["replay_position"],
            "historical_date": snapshot["historical_date"],
            "started_at": snapshot["started_at"],
            "ended_at": snapshot["ended_at"],
            "last_heartbeat": snapshot["last_heartbeat"],
            "end_reason": snapshot["end_reason"],
            "is_active": snapshot["is_active"],
        },
    )

def fail_active_run(error: str) -> None:
    """Finalizo con error el RUN activo cuando falla su inicialización."""

    service = get_active_service()
    if service is not None:
        service.fail(error)

def get_active_service() -> Any:
    """Devuelvo el servicio Alpaca administrado por el controlador."""

    with _MANAGER_LOCK:
        return _ACTIVE_SERVICE

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
                "run_info_file": f"{run_id}_01_python_controller_info.jsonl",
                "log_controller_file": f"{run_id}_01_python_controller_log.txt",
                "log_alpaca_file": f"{run_id}_02_alpaca_stream_log.txt",
                "log_spark_file": f"{run_id}_04_apache_spark_log.txt",
                "kafka_log_file": f"{run_id}_03_apache_kafka_log.txt",
                "log_pandas_postgresql_file": f"{run_id}_05_pandas_postgresql_log.txt",
            }
        )
        return snapshot
    metadata = streaming_module.get_latest_metadata()
    if metadata is not None:
        metadata.setdefault("mode", "REPLAY")
        metadata.setdefault("universe", {"selection": "manual"})
        metadata.setdefault("replay_speed", 1.0)
        metadata.setdefault("replay_position", 0)
        metadata.setdefault("historical_date", None)
        metadata.setdefault("log_count", 0)
        metadata.setdefault("logs", [])
        return metadata
    return {
        "run_id": None,
        "symbols": [],
        "universe": {"selection": "manual", "symbols": []},
        "mode": "REPLAY",
        "replay_speed": 1.0,
        "status": "Listo para ejecutar",
        "error": None,
        "event_count": 0,
        "log_count": 0,
        "replay_position": 0,
        "historical_date": None,
        "logs": [],
        "started_at": None,
        "ended_at": None,
        "last_heartbeat": None,
        "end_reason": None,
        "is_active": False,
    }

def mark_interrupted_run() -> dict[str, Any] | None:
    """Reconozco un RUN activo que pertenecía a otro proceso Python."""

    streaming_module = _require_streaming_module()
    metadata = streaming_module.get_latest_metadata()
    if metadata is None or not metadata.get("is_active"):
        return metadata
    metadata["status"] = "Incompleto"
    metadata["is_active"] = False
    metadata["end_reason"] = "process_restart"
    metadata["ended_at"] = streaming_module.get_utc_timestamp()
    run_id = metadata.get("run_id")
    if isinstance(run_id, str) and isinstance(metadata.get("run_info_file"), str):
        sequence = int(metadata.get("sequence", 0))
        _RUN_SEQUENCES[run_id] = sequence
        append_run_record(
            run_id,
            "state",
            {
                "status": "Incompleto",
                "is_active": False,
                "end_reason": "process_restart",
                "ended_at": metadata["ended_at"],
            },
        )
    return metadata

def register_heartbeat(run_id: str) -> bool:
    """Valido y registro el heartbeat de una sesión visual activa."""

    service = get_active_service()
    return service.heartbeat(run_id) if service is not None else False

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
        if _ACTIVE_SERVICE is service:
            _ACTIVE_SERVICE = None

def configure_streaming_module(streaming_module: Any) -> None:
    """Registro el módulo Alpaca que utiliza el controlador."""

    global _STREAMING_MODULE
    if _STREAMING_MODULE is streaming_module:
        return
    _STREAMING_MODULE = streaming_module
    mark_interrupted_run()

def configure_pipeline_modules(
    kafka_module: Any,
    spark_module: Any,
) -> None:
    """Registro Kafka y Spark para coordinar el flujo del RUN."""

    global _KAFKA_MODULE, _SPARK_MODULE
    _KAFKA_MODULE = kafka_module
    _SPARK_MODULE = spark_module

def shutdown_active_run() -> None:
    """Cierro el RUN administrado cuando finaliza el proceso Python."""

    stop_active_run("process_shutdown")

def start_run(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str = "iex",
    ranking: str | None = None,
    replay_speed: float = 1.0,
) -> Any:
    """Creo un único RUN y coordino su ciclo de vida."""

    global _ACTIVE_SERVICE, _SUPERVISOR_THREAD
    streaming_module = _require_streaming_module()
    if _KAFKA_MODULE is None or _SPARK_MODULE is None:
        raise RuntimeError("Kafka y Spark no fueron configurados.")
    with _MANAGER_LOCK:
        if _ACTIVE_SERVICE is not None and _ACTIVE_SERVICE.is_active():
            return _ACTIVE_SERVICE
        if ranking is not None:
            resolved_symbols, universe_metadata = (
                streaming_module.get_sp500_ranked_symbols(
                    api_key,
                    api_secret,
                    ranking,
                )
            )
        elif streaming_module.ALL_MARKET_SYMBOL in symbols:
            resolved_symbols, universe_metadata = (
                streaming_module.resolve_automatic_symbols(
                    api_key,
                    api_secret,
                )
            )
        else:
            resolved_symbols = symbols
            universe_metadata = {
                "selection": "manual",
                "symbols": list(symbols),
            }
        started_at = streaming_module.get_utc_timestamp()
        run_id = streaming_module.build_run_stem(started_at)
        run_info_path = DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_info.jsonl"
        if run_info_path.exists():
            raise RuntimeError(
                "El identificador del RUN ya existe; esperá un segundo y volvé a iniciar."
            )
        _KAFKA_MODULE.initialize_run(run_id)
        try:
            _ACTIVE_SERVICE = streaming_module.AlpacaStreamingService(
                api_key,
                api_secret,
                run_id,
                resolved_symbols,
                started_at,
                run_id,
                universe_metadata,
                feed,
                replay_speed,
                release_callback=release_active_service,
                publish_events_callback=_KAFKA_MODULE.publish_events,
                append_controller_log_callback=append_controller_log,
                append_run_record_callback=append_run_record,
                append_run_state_callback=append_run_state,
            )
        except FileExistsError as error:
            raise RuntimeError(
                "El identificador del RUN ya existe; esperá un segundo y volvé a iniciar."
            ) from error
        _ACTIVE_SERVICE.start()
        _SUPERVISOR_THREAD = threading.Thread(
            target=_supervise_heartbeat,
            args=(_ACTIVE_SERVICE,),
            daemon=True,
            name=f"controller-supervisor-{run_id}",
        )
        _SUPERVISOR_THREAD.start()
        return _ACTIVE_SERVICE

def stop_active_run(end_reason: str) -> None:
    """Detengo el único RUN administrado por el controlador."""

    global _SUPERVISOR_THREAD
    service = get_active_service()
    if service is not None:
        service.stop(end_reason)
    supervisor_thread = _SUPERVISOR_THREAD
    if (
        supervisor_thread is not None
        and supervisor_thread is not threading.current_thread()
    ):
        supervisor_thread.join(timeout=5)
    _SUPERVISOR_THREAD = None

def _require_streaming_module() -> Any:
    """Obtengo el módulo Alpaca configurado para el controlador."""

    if _STREAMING_MODULE is None:
        raise RuntimeError("El módulo Alpaca no fue configurado.")
    return _STREAMING_MODULE

def _supervise_heartbeat(service: Any) -> None:
    """Detengo el RUN cuando la interfaz deja de informar actividad."""

    while service.is_active():
        time.sleep(1)
        if (
            service.is_active()
            and service.heartbeat_elapsed() > HEARTBEAT_TIMEOUT_SECONDS
        ):
            service.stop("heartbeat_timeout")
            return

atexit.register(shutdown_active_run)
