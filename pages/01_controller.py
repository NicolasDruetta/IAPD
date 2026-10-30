"""Control del ciclo de vida de las ejecuciones del laboratorio."""

import atexit
import threading
import time

from pathlib import Path
from typing import Any

_ACTIVE_SERVICE: Any = None
HEARTBEAT_TIMEOUT_SECONDS = 30
_MANAGER_LOCK = threading.RLock()
_STREAMING_MODULE: Any = None
_SUPERVISOR_THREAD: threading.Thread | None = None

def append_spark_checkpoint(
    run_id: str | None,
    checkpoint: dict[str, Any],
    console_lines: list[str] | None = None,
) -> None:
    """Persisto el acumulado de Spark del RUN correspondiente."""

    streaming_module = _require_streaming_module()
    if run_id is None:
        return
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        service.append_spark_checkpoint(checkpoint, console_lines or [])
        return
    streaming_path = streaming_module.get_run_file(run_id, "info")
    if streaming_path.exists():
        previous = streaming_module.read_spark_checkpoint(streaming_path)
        processed_count = int(checkpoint.get("processed_events", 0))
        if previous is not None and processed_count <= int(
            previous.get("processed_events", 0)
        ):
            return
        state = streaming_module.read_streaming_state(streaming_path)
        sequence = int(state.get("sequence", 0)) if state else 0
        streaming_module.append_stream_record(
            streaming_path,
            run_id,
            sequence + 1,
            "spark_checkpoint",
            checkpoint,
        )
        for line in console_lines or []:
            spark_path = streaming_module.get_run_file(run_id, "spark")
            previous_number = streaming_module.read_console_tail(
                spark_path,
                1,
            )[2]
            with spark_path.open("a", encoding="utf-8") as spark_file:
                spark_file.write(f"[#{previous_number + 1}] {line}\n")

def ensure_spark_console_records(
    run_id: str | None,
    console_lines: list[str],
) -> None:
    """Persisto la primera salida Spark del RUN activo."""

    if run_id is None:
        return
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        service.ensure_spark_console_records(console_lines)

def get_active_service() -> Any:
    """Devuelvo el servicio Alpaca administrado por el controlador."""

    with _MANAGER_LOCK:
        return _ACTIVE_SERVICE

def get_events_after(
    run_id: str | None,
    processed_count: int,
) -> list[Any]:
    """Obtengo los eventos posteriores al checkpoint de Spark."""

    streaming_module = _require_streaming_module()
    if run_id is None:
        return []
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        return service.get_events_after(processed_count)
    return streaming_module.read_events_after(run_id, processed_count)

def get_run_snapshot() -> dict[str, Any]:
    """Obtengo el estado activo o la última ejecución persistida."""

    streaming_module = _require_streaming_module()
    service = get_active_service()
    if service is not None:
        return service.get_snapshot()
    metadata = streaming_module.get_latest_metadata()
    if metadata is not None:
        metadata.setdefault("mode", "LIVE")
        metadata.setdefault("universe", {"selection": "manual"})
        metadata.setdefault("replay_speed", 1.0)
        metadata.setdefault("replay_position", 0)
        metadata.setdefault("historical_date", None)
        metadata.setdefault("log_count", 0)
        metadata.setdefault("logs", [])
        metadata.setdefault("streaming_file", None)
        metadata.setdefault("historical_file", None)
        metadata.setdefault("console_schema_version", 0)
        return metadata
    return {
        "run_id": None,
        "symbols": [],
        "universe": {"selection": "manual", "symbols": []},
        "mode": "LIVE",
        "replay_speed": 1.0,
        "status": "Listo para ejecutar",
        "error": None,
        "event_count": 0,
        "log_count": 0,
        "replay_position": 0,
        "historical_date": None,
        "logs": [],
        "streaming_file": None,
        "historical_file": None,
        "started_at": None,
        "ended_at": None,
        "last_heartbeat": None,
        "end_reason": None,
        "is_active": False,
        "console_schema_version": 0,
    }

def get_spark_checkpoint(run_id: str | None) -> dict[str, Any] | None:
    """Obtengo el checkpoint Spark activo o persistido."""

    streaming_module = _require_streaming_module()
    if run_id is None:
        return None
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        return service.get_spark_checkpoint()
    new_path = streaming_module.get_run_file(run_id, "info")
    if new_path.exists():
        return streaming_module.read_spark_checkpoint(new_path)
    return streaming_module.read_spark_checkpoint(
        streaming_module.LEGACY_STREAMING_DIRECTORY / f"{run_id}.jsonl"
    )

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
    streaming_file = (
        metadata.get("run_info_file")
        if int(metadata.get("console_schema_version", 0)) >= 3
        else metadata.get("streaming_file")
    )
    if isinstance(run_id, str) and isinstance(streaming_file, str):
        streaming_path = (
            streaming_module.DATA_DIRECTORY / streaming_file
            if int(metadata.get("console_schema_version", 0)) >= 3
            else streaming_module.LEGACY_STREAMING_DIRECTORY / streaming_file
        )
        sequence = int(metadata.get("sequence", 0))
        streaming_module.append_stream_record(
            streaming_path,
            run_id,
            sequence + 1,
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
    with _MANAGER_LOCK:
        if _ACTIVE_SERVICE is not None and _ACTIVE_SERVICE.is_active():
            return _ACTIVE_SERVICE
        selected_mode = streaming_module.get_market_mode(
            api_key,
            api_secret,
        )
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
                selected_mode,
                replay_speed,
                release_callback=release_active_service,
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
