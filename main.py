import importlib.util
import components
import functions
import json
import streamlit as st
import sys
import threading

from datetime import datetime, timezone, tzinfo
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

DATA_DIRECTORY = Path(__file__).parent / "data"

CONSOLE_TRANSFER_BATCH_SIZE = 256
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")
SYMBOL_OPTIONS = {
    "Todos los símbolos del S&P 500": {
        "value": ("*",),
        "ranking": None,
    },
    "Las 7 magníficas: AAPL, AMZN, GOOGL, META, MSFT, NVDA, TSLA": {
        "value": ("AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA"),
        "ranking": None,
    },
    "Top 20 con más liquidez del S&P 500": {
        "value": (),
        "ranking": "trades",
    },
    "Top 20 con más volumen del S&P 500": {
        "value": (),
        "ranking": "volume",
    },
}

def format_presentation_timestamp(
    value: Any = None,
    source_timezone: tzinfo | None = None,
) -> str:
    """Convierto un timestamp zonificado a la hora oficial del laboratorio."""

    if value is None or value == "":
        return "-"
    if isinstance(value, datetime):
        timestamp = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            timestamp = datetime.fromisoformat(text)
        except ValueError:
            return str(value)
    if timestamp.tzinfo is None:
        if source_timezone is None:
            return str(value)
        timestamp = timestamp.replace(tzinfo=source_timezone)
    return timestamp.astimezone(PRESENTATION_TIMEZONE).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

def format_presentation_log_line(line: str) -> str:
    """Convierto la marca UTC de un log al horario del laboratorio."""

    timestamp_text, separator, message = line.partition(" | ")
    if not separator:
        return line
    timestamp = format_presentation_timestamp(
        timestamp_text,
        source_timezone=timezone.utc,
    )
    if timestamp == timestamp_text:
        return line
    return f"{timestamp} | {message}"

def format_elapsed_time(
    started_at: str | None,
    ended_at: str | None = None,
) -> str:
    """Formateo el tiempo transcurrido desde el inicio del RUN."""

    if not started_at:
        return "00:00:00"
    try:
        started_timestamp = datetime.fromisoformat(
            started_at.replace("Z", "+00:00")
        )
        end_timestamp = datetime.fromisoformat(
            ended_at.replace("Z", "+00:00")
        ) if ended_at else datetime.now(timezone.utc)
    except ValueError:
        return "00:00:00"
    elapsed_seconds = max(
        0,
        int((end_timestamp - started_timestamp).total_seconds()),
    )
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def get_console_state(console_id: str, run_id: str | None) -> dict[str, Any]:
    """Mantengo el contador visual independiente de cada consola y RUN."""

    key = run_id or "empty"
    state = st.session_state.console_states.setdefault(console_id, {})
    if state.get("run_key") != key:
        state.clear()
        state.update({"run_key": key, "line_number": 0})
    return state

def initialize_state(system_theme: dict[str, Any] | None = None) -> None:
    """Creo el estado para conservar referencias visuales del RUN."""

    detected_dark_mode = (
        system_theme.get("dark")
        if isinstance(system_theme, dict)
        else None
    )
    if "theme_user_set" not in st.session_state:
        st.session_state.theme_user_set = False
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = (
            bool(detected_dark_mode)
            if detected_dark_mode is not None
            else False
        )
    elif (
        not st.session_state.theme_user_set
        and detected_dark_mode is not None
    ):
        st.session_state.dark_mode = bool(detected_dark_mode)
    if "alpaca_run_id" not in st.session_state:
        st.session_state.alpaca_run_id = None
    if "start_requested" not in st.session_state:
        st.session_state.start_requested = False
    if "start_pending" not in st.session_state:
        st.session_state.start_pending = False
    if "start_operation" not in st.session_state:
        st.session_state.start_operation = None
    if "stop_pending" not in st.session_state:
        st.session_state.stop_pending = False
    if "manual_stop_status" not in st.session_state:
        st.session_state.manual_stop_status = False
    if "console_states" not in st.session_state:
        st.session_state.console_states = {}

def load_custom_css() -> None:
    """Cargo los estilos visuales administrados en el archivo del proyecto."""

    css_path = Path(__file__).parent / "assets" / "custom.css"
    css_content = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

def load_page_module(module_name: str) -> ModuleType:
    """Cargo un módulo de procesamiento sin construir su interfaz."""

    module_path = Path(__file__).parent / "pages" / f"{module_name}.py"
    source_mtime_ns = module_path.stat().st_mtime_ns
    cached_module = sys.modules.get(module_name)
    if cached_module is not None:
        cached_mtime_ns = getattr(
            cached_module,
            "_SOURCE_MTIME_NS",
            None,
        )
        active_service = getattr(
            cached_module,
            "get_active_service",
            lambda: None,
        )()
        if active_service is None and module_name == "02_alpaca_stream":
            controller_module = sys.modules.get("01_python_controller")
            active_service = getattr(
                controller_module,
                "get_active_service",
                lambda: None,
            )()
        if (
            cached_mtime_ns == source_mtime_ns
            or (
                active_service is not None
                and active_service.is_active()
            )
            or (
                module_name == "04_apache_spark"
                and _has_active_spark_query(cached_module)
            )
        ):
            return cached_module
        sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(
        module_name, module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError("No se pudo cargar el módulo de Alpaca")
    module = importlib.util.module_from_spec(spec)
    module.DATA_DIRECTORY = DATA_DIRECTORY
    module.PRESENTATION_TIMEZONE = PRESENTATION_TIMEZONE
    module.format_presentation_timestamp = format_presentation_timestamp
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        module._SOURCE_MTIME_NS = source_mtime_ns
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module

def _has_active_spark_query(module: ModuleType) -> bool:
    """Consulto Spark sin propagar errores de conexión a la interfaz."""

    try:
        return bool(
            getattr(module, "has_active_query", lambda: False)()
        )
    except Exception:
        return False

def mark_start_pending() -> None:
    """Marco que el usuario acaba de solicitar el inicio."""

    st.session_state.manual_stop_status = False
    st.session_state.start_requested = True
    st.session_state.start_pending = True

def handle_start_error(
    error: Exception,
    controller_module: ModuleType,
    spark_module: ModuleType,
    run_id: str | None,
) -> None:
    """Finalizo el RUN y conservo el error visible tras un fallo de inicio."""

    error_message = " ".join(str(error).split()) or error.__class__.__name__
    if run_id is not None:
        spark_module.stop_streaming(run_id, finalize=False)
    controller_module.fail_active_run(error_message)
    st.session_state.start_pending = False
    st.session_state.start_operation = None
    st.session_state.alpaca_run_id = None

def start_run_in_background(
    operation: dict[str, Any],
    controller_module: ModuleType,
    streaming_module: ModuleType,
    symbols: list[str],
    symbol_ranking: str | None,
    replay_speed: float,
) -> None:
    """Inicio el RUN sin bloquear el refresco visual de Streamlit."""

    try:
        operation["service"] = controller_module.start_run(
            streaming_module.ALPACA_API_KEY,
            streaming_module.ALPACA_SECRET_KEY,
            symbols,
            ranking=symbol_ranking,
            replay_speed=replay_speed,
        )
    except Exception as error:
        operation["error"] = error
    finally:
        operation["done"] = True

def mark_stop_pending() -> None:
    """Marco que el usuario acaba de solicitar la detención."""

    st.session_state.stop_pending = True

def render_log_console(
    lines: list[str],
    console_id: str,
    cursor_end: int,
    cursor_start: int = 0,
    has_more: bool = False,
    initial_load: bool = False,
    placeholder: bool = False,
    resync: bool = False,
) -> None:
    """Muestro y actualizo incrementalmente una consola de registros."""

    components.LOG_CONSOLE_COMPONENT(
        data={"lines": lines},
        height=250,
        key=console_id,
    )

def get_console_buffer_view(
    run_id: str | None,
    console_name: str,
    console_path: Path | None,
    console_state: dict[str, Any],
    clear_consoles: bool,
) -> tuple[list[str], int, int, bool, bool]:
    """Leo solamente los 200 últimos registros del TXT de cada consola."""

    if clear_consoles or run_id is None or console_path is None:
        console_state["line_number"] = 0
        return [], 0, 0, True, False
    lines, _, total = functions.read_console_tail(console_path, 200)
    console_state["line_number"] = total
    return lines, 0, total, True, False

def render_streaming() -> None:
    """Muestro las consolas de Alpaca y el estado de Spark."""

    controller_module = load_page_module("01_python_controller")
    streaming_module = load_page_module("02_alpaca_stream")
    kafka_module = load_page_module("03_apache_kafka")
    spark_module = load_page_module("04_apache_spark")
    controller_module.configure_streaming_module(streaming_module)
    controller_module.configure_pipeline_modules(
        kafka_module,
        spark_module,
    )
    snapshot = controller_module.get_run_snapshot()
    active_run_id = snapshot["run_id"] if snapshot["is_active"] else None
    session_run_id = st.session_state.alpaca_run_id
    if session_run_id is None and active_run_id is not None:
        st.session_state.alpaca_run_id = active_run_id
        session_run_id = active_run_id
    if session_run_id == active_run_id and active_run_id is not None:
        controller_module.register_heartbeat(active_run_id)
        snapshot = controller_module.get_run_snapshot()

    universe = snapshot.get("universe", {})
    option_labels = list(SYMBOL_OPTIONS)
    default_option = option_labels[0]
    if snapshot["is_active"] and universe.get("selection") == "manual":
        manual_symbols = tuple(snapshot["symbols"])
        default_option = next(
            (
                label
                for label, option in SYMBOL_OPTIONS.items()
                if option["value"] == manual_symbols
            ),
            default_option,
        )
    st.subheader("Símbolos bursátiles a analizar")
    symbols_col, actions_col = st.columns(
        [1, 1],
        gap="small",
        vertical_alignment="bottom",
    )
    with symbols_col:
        with st.container(key="card-symbols"):
            selected_option = st.selectbox(
                "Símbolos",
                options=option_labels,
                index=option_labels.index(default_option),
                key="symbol-universe",
                label_visibility="collapsed",
                disabled=bool(
                    snapshot["is_active"]
                    or st.session_state.start_pending
                    or st.session_state.stop_pending
                ),
                width="stretch",
            )
    stop_clicked = False
    with actions_col:
        with st.container(
            horizontal=True,
            horizontal_alignment="left",
            vertical_alignment="bottom",
            gap="small",
        ):
            start_clicked = st.button(
                "⟳ Iniciando..."
                if st.session_state.start_pending
                else (
                    "⟳ Ejecutando streaming..."
                    if snapshot["is_active"]
                    else "▶ Iniciar streaming"
                ),
                disabled=bool(
                    snapshot["is_active"]
                    or st.session_state.start_pending
                ),
                key="start-button",
                on_click=mark_start_pending,
                width="content",
            )
            if (
                snapshot["is_active"]
                and not st.session_state.start_pending
            ) or st.session_state.stop_pending:
                stop_clicked = st.button(
                    "⟳ Deteniendo..."
                    if st.session_state.stop_pending
                    else "■ Detener streaming",
                    disabled=st.session_state.stop_pending,
                    key="stop-button",
                    on_click=mark_stop_pending,
                    width="content",
                )
    selected_symbols = SYMBOL_OPTIONS[selected_option]
    symbols = list(selected_symbols["value"])
    symbol_ranking = selected_symbols["ranking"]
    selected_replay_speed = 1.0
    clear_consoles = bool(
        st.session_state.start_requested
        or st.session_state.start_pending
    )

    if st.session_state.start_pending:
        if (
            not streaming_module.ALPACA_API_KEY
            or not streaming_module.ALPACA_SECRET_KEY
        ):
            st.session_state.start_pending = False
        else:
            operation = st.session_state.start_operation
            if operation is None:
                operation = {
                    "done": False,
                    "error": None,
                    "service": None,
                }
                start_thread = threading.Thread(
                    target=start_run_in_background,
                    args=(
                        operation,
                        controller_module,
                        streaming_module,
                        symbols,
                        symbol_ranking,
                        selected_replay_speed,
                    ),
                    daemon=True,
                    name="start-streaming-run",
                )
                operation["thread"] = start_thread
                st.session_state.start_operation = operation
                start_thread.start()
            elif not operation["thread"].is_alive():
                if operation["error"] is not None:
                    st.session_state.start_pending = False
                    st.session_state.start_operation = None
                else:
                    service = operation["service"]
                    run_id = service.get_snapshot()["run_id"]
                    try:
                        st.session_state.alpaca_run_id = run_id
                        controller_module.register_heartbeat(run_id)
                        kafka_module.ensure_topic()
                        spark_module.start_streaming(
                            run_id,
                            service.get_snapshot()["mode"],
                        )
                        service.release_pipeline()
                        st.session_state.start_pending = False
                        st.session_state.start_operation = None
                        snapshot = controller_module.get_run_snapshot()
                    except Exception as error:
                        handle_start_error(
                            error,
                            controller_module,
                            spark_module,
                            run_id,
                        )
                        snapshot = controller_module.get_run_snapshot()

    if stop_clicked or st.session_state.stop_pending:
        st.session_state.stop_pending = False
        st.session_state.manual_stop_status = True
        st.session_state.start_requested = False
        clear_consoles = False
        run_id = snapshot.get("run_id")
        controller_module.stop_active_run("manual_stop")
        snapshot = controller_module.get_run_snapshot()
        spark_module.stop_streaming(run_id)

    if st.session_state.start_pending:
        elapsed_text = "-"
        market_date = "-"
        market_time = "-"
    else:
        elapsed_text = format_elapsed_time(
            snapshot["started_at"],
            snapshot["ended_at"],
        )
        market_timestamp = snapshot.get("market_timestamp")
        presentation_market_timestamp = format_presentation_timestamp(
            market_timestamp,
        )
        market_date = (
            snapshot.get("historical_date") or "-"
            if snapshot["is_active"]
            else "-"
        )
        market_time = (
            presentation_market_timestamp.split(" ", 1)[1]
            if presentation_market_timestamp != "-"
            else "-"
        )
    status = snapshot["status"]
    status_text = "Listo para ejecutar."
    if st.session_state.manual_stop_status:
        status_text = "Detenido manualmente."
    elif st.session_state.start_pending:
        status_text = "Iniciando..."
    elif snapshot["is_active"]:
        status_text = f"Ejecutando... {elapsed_text}."
    elif status == "Error":
        status_text = "Finalizado con error."
    mode_col, run_col, status_col = st.columns(
        [1, 1, 2],
        gap="small",
    )
    with mode_col:
        with st.container(border=True):
            st.caption("Fecha de mercado")
            st.text(market_date)
    with run_col:
        run_id = snapshot["run_id"] if snapshot["is_active"] else None
        with st.container(border=True):
            st.caption("RUN ID")
            st.text(run_id if run_id else "-")
    with status_col:
        with st.container(border=True):
            st.caption("Estado")
            if snapshot["is_active"] and not st.session_state.start_pending:
                components.EXECUTION_TIMER_COMPONENT(
                    data={
                        "active": True,
                        "ended_at": snapshot["ended_at"],
                        "started_at": snapshot["started_at"],
                        "text": status_text,
                        "timer_key": snapshot["run_id"],
                        "prefix": "Ejecutando...",
                    },
                    key="execution-timer",
                )
            else:
                st.text(status_text)

    if snapshot["error"] and not st.session_state.start_pending:
        st.error("**Error:** " + (" ".join(str(snapshot["error"]).split())))

    st.divider()
    st.write("")

    last_execution_notice = None
    if (
        snapshot["run_id"]
        and not snapshot["is_active"]
        and not st.session_state.start_pending
    ):
        last_execution_at = snapshot["ended_at"] or snapshot["started_at"]
        last_execution_text = (
            format_presentation_timestamp(last_execution_at)
            if last_execution_at
            else "-"
        )
        elapsed_text = format_elapsed_time(
            snapshot["started_at"],
            snapshot["ended_at"],
        )
        history_mode = snapshot["mode"] or "-"
        history_run_id = snapshot["run_id"] or "-"
        completion_text = {
            "Completado": "Finalizado correctamente.",
            "Error": "Finalizado con error.",
        }.get(snapshot["status"], f"{snapshot['status']}.")
        if (
            snapshot["status"] == "Detenido"
            and snapshot.get("end_reason") == "manual_stop"
        ):
            completion_text = "Detenido manualmente."
        last_execution_notice = f"Datos de la **última ejecución:** {last_execution_text}. **Duración:** {elapsed_text}. **{completion_text}** | **Modo:** {history_mode}. | **RUN:** {history_run_id}."
        automatic_stop_messages = {
            "heartbeat_timeout": "La ejecución se detuvo automáticamente porque la interfaz dejó de enviar señales de actividad durante 15 minutos.",
            "process_restart": "La ejecución se detuvo automáticamente porque se reinició el proceso de la aplicación.",
            "process_shutdown": "La ejecución se detuvo automáticamente porque se cerró el proceso de la aplicación.",
        }
        automatic_stop_reason = snapshot.get("end_reason")
        automatic_stop_message = automatic_stop_messages.get(
            automatic_stop_reason,
        )
        if automatic_stop_message and automatic_stop_reason != "manual_stop":
            st.error(automatic_stop_message)
        st.info(last_execution_notice)

    run_id = None if st.session_state.start_pending else snapshot["run_id"]
    if run_id is not None and snapshot["is_active"]:
        try:
            kafka_module.ensure_topic()
            spark_module.start_streaming(run_id, snapshot["mode"])
            controller_module.release_active_pipeline(run_id)
        except Exception as error:
            handle_start_error(
                error,
                controller_module,
                spark_module,
                run_id,
            )
            snapshot = controller_module.get_run_snapshot()
            run_id = None
    elif run_id is not None and spark_module.has_active_query():
        spark_module.stop_streaming(
            run_id,
            finalize=snapshot.get("status") == "Completado",
        )
    spark_result = spark_module.get_snapshot(run_id)
    execution_column, alpaca_column = st.columns(
        2,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with execution_column:
        st.subheader("1. Log del controlador (Python clásico)")
        execution_state = get_console_state(
            "execution-log-console",
            run_id,
        )
        streaming_file = snapshot.get("streaming_file")
        streaming_path = (
            DATA_DIRECTORY / "runs" / streaming_file
            if streaming_file
            else None
        )
        execution_path = (
            DATA_DIRECTORY / "runs" / f"{run_id}_01_controller_log.txt"
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        alpaca_path = (
            DATA_DIRECTORY / "runs" / f"{run_id}_02_alpaca_log.txt"
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        kafka_path = kafka_module.get_log_path(run_id) if run_id else None
        spark_path = (
            DATA_DIRECTORY / "runs" / f"{run_id}_04_spark_log.txt"
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        downloads_disabled = any(
            path is None or not path.exists()
            for path in (
                execution_path,
                alpaca_path,
                kafka_path,
                spark_path,
            )
        )
        log_lines, cursor_start, next_cursor, initial_load, resync = (
            get_console_buffer_view(
                run_id,
                "controller",
                execution_path,
                execution_state,
                clear_consoles,
            )
        )
        log_display_lines = [
            format_presentation_log_line(line)
            for line in log_lines
        ]
        is_placeholder = (
            not log_display_lines
            and execution_state["line_number"] == 0
        )
        if is_placeholder:
            log_display_lines = [
                "Log del controlador | esperando actividad."
            ]
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
        ):
            st.caption(
                f"Eventos recibidos: {execution_state['line_number']}"
            )
            st.download_button(
                "↓",
                data=execution_path.read_bytes()
                if execution_path is not None and execution_path.exists()
                else b"",
                file_name=(
                    execution_path.name
                    if execution_path is not None
                    else "01_controller_log.txt"
                ),
                mime="text/plain",
                key=f"download-execution-log-console-{run_id or 'no-run'}",
                help="Descargar log completo",
                disabled=downloads_disabled,
                type="secondary",
                width=36,
            )
        render_log_console(
            log_display_lines,
            "execution-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=False,
            initial_load=initial_load,
            placeholder=is_placeholder,
            resync=resync,
        )
    with alpaca_column:
        st.subheader("2. Log de descarga de datos (Alpaca)")
        alpaca_state = get_console_state(
            "alpaca-log-console",
            run_id,
        )
        events, cursor_start, next_cursor, initial_load, resync = (
            get_console_buffer_view(
                run_id,
                "alpaca",
                alpaca_path,
                alpaca_state,
                clear_consoles,
            )
        )
        event_lines = [
            format_presentation_log_line(line)
            for line in events
        ]
        has_event_lines = bool(event_lines)
        is_placeholder = (
            not event_lines
            and alpaca_state["line_number"] == 0
        )
        if is_placeholder:
            event_lines = [
                "Alpaca | esperando cotizaciones u operaciones."
            ]
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
        ):
            st.caption(f"Datos descargados: {alpaca_state['line_number']}")
            st.download_button(
                "↓",
                data=alpaca_path.read_bytes()
                if alpaca_path is not None and alpaca_path.exists()
                else b"",
                file_name=(
                    alpaca_path.name
                    if alpaca_path is not None
                    else "02_alpaca_log.txt"
                ),
                mime="text/plain",
                key=f"download-alpaca-log-console-{run_id or 'no-run'}",
                help="Descargar log completo",
                disabled=downloads_disabled,
                type="secondary",
                width=36,
            )
        render_log_console(
            event_lines,
            "alpaca-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=False,
            initial_load=initial_load,
            placeholder=is_placeholder,
            resync=resync,
        )
    kafka_column, spark_column = st.columns(
        2,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with kafka_column:
        st.subheader("3. Log de publicación (Kafka)")
        kafka_state = get_console_state("kafka-log-console", run_id)
        kafka_records, cursor_start, next_cursor, initial_load, resync = (
            get_console_buffer_view(
                run_id,
                "kafka",
                kafka_path,
                kafka_state,
                clear_consoles,
            )
        )
        kafka_lines = [
            format_presentation_log_line(line)
            for line in kafka_records
        ]
        kafka_cursor_end = next_cursor
        is_placeholder = not kafka_lines and kafka_state["line_number"] == 0
        if is_placeholder:
            kafka_lines = ["Kafka | esperando eventos para publicar."]
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
        ):
            st.caption(f"Registros publicados: {kafka_state['line_number']}")
            st.download_button(
                "↓",
                data=kafka_path.read_bytes()
                if kafka_path is not None and kafka_path.exists()
                else b"",
                file_name=(
                    kafka_path.name
                    if kafka_path is not None
                    else "log_03_apache_kafka.txt"
                ),
                mime="text/plain",
                key=f"download-kafka-log-console-{run_id or 'no-run'}",
                help="Descargar log completo",
                disabled=downloads_disabled,
                type="secondary",
                width=36,
            )
        render_log_console(
            kafka_lines,
            "kafka-log-console",
            cursor_end=kafka_cursor_end,
            cursor_start=cursor_start,
            has_more=False,
            initial_load=initial_load,
            placeholder=is_placeholder,
            resync=resync,
        )
    with spark_column:
        st.subheader("4. Log de procesamiento (Spark)")
        spark_state = get_console_state("spark-log-console", run_id)
        spark_records, cursor_start, next_cursor, initial_load, resync = (
            get_console_buffer_view(
                run_id,
                "spark",
                spark_path,
                spark_state,
                clear_consoles,
            )
        )
        spark_lines = [format_presentation_log_line(line) for line in spark_records]
        is_placeholder = not spark_lines and spark_state["line_number"] == 0
        if is_placeholder:
            spark_lines = ["Spark | esperando eventos bursátiles."]
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
        ):
            st.caption(f"Registros procesados: {spark_state['line_number']}")
            st.download_button(
                "↓",
                data=spark_path.read_bytes()
                if spark_path is not None and spark_path.exists()
                else b"",
                file_name=(
                    spark_path.name
                    if spark_path is not None
                    else "04_spark_log.txt"
                ),
                mime="text/plain",
                key=f"download-spark-log-console-{run_id or 'no-run'}",
                help="Descargar log completo",
                disabled=downloads_disabled,
                type="secondary",
                width=36,
            )
        render_log_console(
            spark_lines,
            "spark-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=False,
            initial_load=initial_load,
            placeholder=is_placeholder,
            resync=resync,
        )
        st.caption(
            "**[Spark Master ↗](http://localhost:8080/)** &nbsp;-&nbsp; "
            "**[Worker 1 ↗](http://localhost:8081/)** &nbsp;-&nbsp; "
            "**[Worker 2 ↗](http://localhost:8082/)** &nbsp;-&nbsp; "
            "**[Spark Jobs ↗](http://localhost:4040/)** "
        )
    if clear_consoles:
        st.session_state.start_requested = False

    st.divider()
    st.subheader("5. Gráficos en vivo (Streamlit)")
    st.write("")
    
    if last_execution_notice:
        st.info(last_execution_notice)
    with st.container(border=True):
        date_col, time_col, elapsed_col, data_col = st.columns(
            [1, 1, 1, 1],
            gap="small",
            vertical_alignment="top",
        )
        with date_col:
            st.caption("Fecha de mercado analizada")
            st.text(market_date)
        with time_col:
            st.caption("Última hora procesada")
            st.text(market_time)
        with elapsed_col:
            st.caption("Tiempo de ejecución transcurrido")
            if snapshot["is_active"] and not st.session_state.start_pending:
                components.EXECUTION_TIMER_COMPONENT(
                    data={
                        "active": True,
                        "ended_at": snapshot["ended_at"],
                        "prefix": "",
                        "started_at": snapshot["started_at"],
                        "text": elapsed_text,
                        "timer_key": snapshot["run_id"],
                    },
                    key="execution-timer-elapsed",
                )
            else:
                st.text(elapsed_text)
        with data_col:
            st.caption("Cantidad de datos descargados")
            st.text(str(snapshot.get("event_count", 0)))
    charts_module = load_page_module("05_streamlit_charts")
    top_volume = spark_result.get("top_volume", [])
    activity_rows, activity_interval = charts_module.get_activity_chart_rows(
        spark_result.get("activity_rows", []),
    )
    volume_column, activity_column = st.columns(
        2,
        gap="small",
        vertical_alignment="top",
    )
    with volume_column:
        st.subheader("Top 10 acciones por volumen negociado")
        charts_module.render_volume_chart(top_volume)
    with activity_column:
        st.subheader("Actividad bursátil en el tiempo")
        charts_module.render_activity_chart(activity_rows, activity_interval)

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="assets/icon.png",
    layout="wide",
)

load_custom_css()
system_theme = components.SYSTEM_THEME_COMPONENT(
    data={},
    default={"dark": None},
    key="system-theme-detector",
    on_dark_change=lambda: None,
)
initialize_state(system_theme)
icon_path = Path(__file__).parent / "assets" / "icon.png"
theme_key = "theme-dark" if st.session_state.dark_mode else "theme-light"
with st.container(key=theme_key):
    st.empty()
with st.container(
    width="content",
    horizontal=True,
    gap="small",
    vertical_alignment="center",
):
    icon_col = st.container(width="content")
    title_col = st.container(width="stretch")
    theme_col = st.container(width="content")
with icon_col:
    st.image(str(icon_path), width=50)
with title_col:
    st.title("Apache Big Data Lab")
with theme_col:
    if st.button(
        "",
        icon=(
            ":material/light_mode:"
            if st.session_state.dark_mode
            else ":material/dark_mode:"
        ),
        help=(
            "Light Mode"
            if st.session_state.dark_mode
            else "Dark Mode"
        ),
        key="theme-toggle-button",
        width=36,
    ):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.session_state.theme_user_set = True
        st.rerun()
st.subheader(
    "Streaming y análisis de operaciones bursátiles con procesamiento distribuido durante una rueda de mercado."
)
st.caption("Materia: IAPD. Autor: Nicolás Druetta. Año: 2026.")
st.divider()

render_streaming_fragment = st.fragment(render_streaming, run_every="5s")
render_streaming_fragment()
