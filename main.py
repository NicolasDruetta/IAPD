"""Interfaz principal de Apache Big Data Lab."""

import importlib.util
import streamlit as st
import sys

from datetime import datetime, timezone, tzinfo
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

CONSOLE_TRANSFER_BATCH_SIZE = 256
LOG_CONSOLE_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.log_console",
    html=(
        '<textarea class="log-console" readonly '
        'aria-label="Consola"></textarea>'
    ),
    isolate_styles=False,
    js="""
    export default function ({ parentElement, data, setStateValue }) {
        const consoleElement = parentElement.querySelector("textarea");
        const state = parentElement.__logConsoleState || {
            appliedCursor: 0,
            lastBatchId: null,
            lastRequestedCursor: null,
            runKey: null,
        };
        const sameRun = state.runKey === data.run_key;
        if (!sameRun || data.reset) {
            consoleElement.value = "";
            state.appliedCursor = 0;
            state.lastBatchId = null;
            state.lastRequestedCursor = null;
            state.runKey = data.run_key;
        }
        const lines = Array.isArray(data.lines) ? data.lines : [];
        const cursorStart = Number(data.cursor_start || 0);
        const cursorEnd = Number(data.cursor_end || cursorStart);
        const isNewBatch = data.batch_id !== state.lastBatchId;
        const scrollToBottom = () => {
            consoleElement.scrollTop = consoleElement.scrollHeight;
        };
        if (isNewBatch && cursorStart === state.appliedCursor) {
            if (data.initial_load) {
                consoleElement.value = lines.join("\\n");
            } else if (lines.length > 0) {
                const separator = consoleElement.value.length > 0 ? "\\n" : "";
                const suffix = separator + lines.join("\\n");
                consoleElement.value += suffix;
            }
            state.appliedCursor = cursorEnd;
            state.lastBatchId = data.batch_id;
            scrollToBottom();
            requestAnimationFrame(scrollToBottom);
        }
        if (
            data.has_more
            && state.lastRequestedCursor !== state.appliedCursor
        ) {
            state.lastRequestedCursor = state.appliedCursor;
            setStateValue("need_more", state.appliedCursor);
        }
        parentElement.__logConsoleState = state;
        return () => {};
    }
    """,
)
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")

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
    """Obtengo el cursor visual independiente de cada consola y RUN."""

    state_key = run_id or "empty"
    states = st.session_state.console_states
    state = states.setdefault(
        console_id,
        {
            "cursor": 0,
            "line_number": 0,
            "persisted": False,
            "run_key": state_key,
            "signature": None,
            "initialized": False,
        },
    )
    if state["run_key"] != state_key:
        state.clear()
        state.update(
            {
                "cursor": 0,
                "line_number": 0,
                "persisted": False,
                "run_key": state_key,
                "signature": None,
                "initialized": False,
            }
        )
    return state

def initialize_state() -> None:
    """Creo el estado para conservar referencias visuales del RUN."""

    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False
    if "alpaca_run_id" not in st.session_state:
        st.session_state.alpaca_run_id = None
    if "start_requested" not in st.session_state:
        st.session_state.start_requested = False
    if "start_pending" not in st.session_state:
        st.session_state.start_pending = False
    if "stop_pending" not in st.session_state:
        st.session_state.stop_pending = False
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
        if (
            cached_mtime_ns == source_mtime_ns
            or (
                active_service is not None
                and active_service.is_active()
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

def mark_start_pending() -> None:
    """Marco que el usuario acaba de solicitar el inicio."""

    st.session_state.start_requested = True
    st.session_state.start_pending = True

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
) -> None:
    """Muestro y actualizo incrementalmente una consola de registros."""

    run_key = st.session_state.console_states[console_id]["run_key"]
    payload = {
        "batch_id": f"{run_key}:{cursor_end}",
        "cursor_end": cursor_end,
        "cursor_start": cursor_start,
        "follow": False,
        "has_more": has_more,
        "initial_load": initial_load,
        "lines": lines,
        "reset": cursor_start == 0,
        "run_key": run_key,
    }
    LOG_CONSOLE_COMPONENT(
        data=payload,
        default={"need_more": None},
        height=420,
        key=console_id,
        on_need_more_change=lambda: None,
    )

def render_volume_chart(rows: list[dict[str, Any]]) -> None:
    """Muestro el gráfico con el acumulado confirmado por Spark."""

    chart_rows = [
        {
            "symbol": row["symbol"],
            "volume": row["total_volume"],
            "volume_label": f"{row['total_volume']:,.0f}",
        }
        for row in rows
    ]
    chart_spec = {
        "data": {"values": chart_rows},
        "height": 360,
        "layer": [
            {
                "mark": {"type": "bar", "cornerRadiusEnd": 4},
                "encoding": {
                    "y": {
                        "field": "symbol",
                        "type": "nominal",
                        "sort": {
                            "field": "volume",
                            "order": "descending",
                        },
                        "title": "Símbolo",
                    },
                    "x": {
                        "field": "volume",
                        "type": "quantitative",
                        "title": "Volumen acumulado (acciones)",
                    },
                },
            },
            {
                "mark": {"type": "text", "align": "left", "dx": 4},
                "encoding": {
                    "y": {
                        "field": "symbol",
                        "type": "nominal",
                        "sort": {
                            "field": "volume",
                            "order": "descending",
                        },
                    },
                    "x": {
                        "field": "volume",
                        "type": "quantitative",
                    },
                    "text": {
                        "field": "volume_label",
                        "type": "nominal",
                    },
                },
            },
        ],
    }
    st.vega_lite_chart(
        spec=chart_spec,
        use_container_width=True,
    )

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="assets/icon.png",
    layout="wide",
)
load_custom_css()
initialize_state()
icon_path = Path(__file__).parent / "assets" / "icon.png"
theme_key = "theme-dark" if st.session_state.dark_mode else "theme-light"
with st.container(key=theme_key):
    st.empty()
with st.container(width="content"):
    icon_col, title_col, theme_col = st.columns(
        [0.9, 4.9, 0.5],
        gap="small",
        vertical_alignment="center",
    )
    with icon_col:
        st.image(str(icon_path), width=65)
    with title_col:
        st.title("Apache Big Data Lab")
    with theme_col:
        with st.container(key="theme-toggle"):
            if st.button("◐", width="stretch"):
                st.session_state.dark_mode = not st.session_state.dark_mode
                st.rerun()
st.subheader("Procesamiento distribuido y análisis de mercados financieros")
st.caption("Alumno: Nicolás Druetta. Materia: IAPD. Año: 2026.")
st.divider()

def render_streaming() -> None:
    """Muestro las consolas de Alpaca y el estado de Spark."""

    streaming_module = load_page_module("01_alpaca_streaming")
    spark_module = load_page_module("02_apache_spark")
    snapshot = streaming_module.get_run_snapshot()
    active_run_id = snapshot["run_id"] if snapshot["is_active"] else None
    session_run_id = st.session_state.alpaca_run_id
    if session_run_id is None and active_run_id is not None:
        st.session_state.alpaca_run_id = active_run_id
        session_run_id = active_run_id
    if session_run_id == active_run_id and active_run_id is not None:
        streaming_module.register_heartbeat(active_run_id)
        snapshot = streaming_module.get_run_snapshot()

    universe = snapshot.get("universe", {})
    default_symbols = ""
    if snapshot["is_active"] and universe.get("selection") == "manual":
        default_symbols = ", ".join(snapshot["symbols"])
    st.subheader("Símbolos")
    st.caption("Vacío = componentes actuales del S&P 500.")
    symbols_col, actions_col = st.columns(
        [6, 6],
        gap="small",
        vertical_alignment="bottom",
    )
    with symbols_col:
        with st.container(key="card-symbols"):
            symbols_text = st.text_input(
                "Símbolos",
                value=default_symbols,
                placeholder="AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA",
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
        start_col, stop_col, spacer_col = st.columns(
            [2, 2, 2],
            gap="small",
            vertical_alignment="bottom",
        )
        with start_col:
            with st.container(key="start-button"):
                start_clicked = st.button(
                    "⟳ Iniciando..."
                    if st.session_state.start_pending
                    else (
                        "⟳ Ejecutando streaming..."
                        if snapshot["is_active"]
                        else "▶ Iniciar streaming"
                    ),
                    type="secondary",
                    disabled=bool(
                        snapshot["is_active"]
                        or st.session_state.start_pending
                    ),
                    on_click=mark_start_pending,
                    width="stretch",
                )
        if snapshot["is_active"] or st.session_state.stop_pending:
            with stop_col:
                with st.container(key="stop-button"):
                    stop_clicked = st.button(
                        "⟳ Deteniendo..."
                        if st.session_state.stop_pending
                        else "■ Detener streaming",
                        type="secondary",
                        disabled=st.session_state.stop_pending,
                        on_click=mark_stop_pending,
                        width="stretch",
                    )
        with spacer_col:
            st.empty()
    symbols = sorted(
        {
            symbol.strip().upper()
            for symbol in symbols_text.split(",")
            if symbol.strip()
        }
    )
    if not symbols:
        symbols = [getattr(streaming_module, "ALL_MARKET_SYMBOL", "*")]
    selected_replay_speed = 1.0
    clear_consoles = st.session_state.start_requested

    if st.session_state.start_pending and not clear_consoles:
        st.session_state.start_pending = False
        if (
            not streaming_module.ALPACA_API_KEY
            or not streaming_module.ALPACA_SECRET_KEY
        ):
            st.error(
                "Completá ALPACA_API_KEY y ALPACA_SECRET_KEY en "
                "pages/01_alpaca_streaming.py."
            )
        else:
            try:
                service = streaming_module.start_run(
                    streaming_module.ALPACA_API_KEY,
                    streaming_module.ALPACA_SECRET_KEY,
                    symbols,
                    replay_speed=selected_replay_speed,
                )
            except RuntimeError as error:
                st.error(str(error))
            else:
                st.session_state.alpaca_run_id = service.get_snapshot()[
                    "run_id"
                ]
                streaming_module.register_heartbeat(
                    st.session_state.alpaca_run_id
                )
                snapshot = streaming_module.get_run_snapshot()

    if stop_clicked or st.session_state.stop_pending:
        st.session_state.stop_pending = False
        streaming_module.stop_active_run("manual_stop")
        snapshot = streaming_module.get_run_snapshot()

    status = snapshot["status"]
    status_text = "Ejecutando..." if snapshot["is_active"] else status
    elapsed_text = format_elapsed_time(
        snapshot["started_at"],
        snapshot["ended_at"],
    )
    if snapshot["run_id"] is None:
        status_text = "Listo para ejecutar."
    elif snapshot["is_active"]:
        status_text = f"{status_text} {elapsed_text}."
    else:
        last_execution_at = snapshot["ended_at"] or snapshot["started_at"]
        last_execution_text = (
            format_presentation_timestamp(last_execution_at)
            if last_execution_at
            else "-"
        )
        status_text = (
            f"{status_text}. Transcurrido: {elapsed_text}. "
            f"Ejecutado: {last_execution_text}."
        )
    metrics_col, status_col = st.columns(
        [6, 6],
        gap="small",
    )
    with metrics_col:
        run_col, mode_col, count_col = st.columns(
            [2, 2, 2],
            gap="small",
        )
        with run_col:
            run_id = snapshot["run_id"] or "-"
            with st.container(border=True):
                st.caption("RUN")
                st.text(run_id[:12])
        with mode_col:
            with st.container(border=True):
                st.caption("Modo")
                st.text(snapshot["mode"])
        with count_col:
            with st.container(border=True):
                st.caption("Eventos recibidos Alpaca")
                st.text(str(snapshot["event_count"]))
    with status_col:
        with st.container(border=True):
            st.caption("Estado")
            st.text(status_text)

    if snapshot["error"]:
        st.error(snapshot["error"])

    st.divider()

    run_id = snapshot["run_id"]
    spark_checkpoint = streaming_module.get_spark_checkpoint(run_id)
    processed_events = int(
        (spark_checkpoint or {}).get("processed_events", 0)
    )
    spark_events = streaming_module.get_events_after(
        run_id,
        processed_events,
    )
    spark_result = spark_module.process_events(
        spark_events,
        run_id=run_id,
        checkpoint=spark_checkpoint,
    )
    spark_console_lines = list(spark_result["logs"])
    if spark_result["rows"]:
        spark_console_lines.append("")
        spark_console_lines.append("Resultados por símbolo:")
        spark_console_lines.extend(
            "{symbol} | operaciones={operations} | volumen={volume}".format(
                symbol=row["symbol"],
                operations=row["operations"],
                volume=row["total_volume"] or 0,
            )
            for row in spark_result["rows"]
        )
    if spark_result.get("checkpoint") is not None:
        streaming_module.append_spark_checkpoint(
            run_id,
            spark_result["checkpoint"],
            spark_console_lines,
        )
    else:
        streaming_module.ensure_spark_console_records(
            run_id,
            spark_console_lines,
        )
    execution_column, alpaca_column, spark_column = st.columns(
        3,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with execution_column:
        st.subheader("Log de ejecución")
        execution_state = get_console_state(
            "execution-log-console",
            run_id,
        )
        streaming_file = snapshot.get("streaming_file")
        streaming_path = (
            streaming_module.DATA_DIRECTORY / streaming_file
            if streaming_file
            else None
        )
        if streaming_path is not None:
            if clear_consoles:
                cursor_start, next_cursor, has_more = 0, 0, False
                initial_load, log_lines = True, []
            else:
                cursor_start = int(execution_state["cursor"])
                initial_load = not execution_state["initialized"]
                log_lines, next_cursor, has_more = (
                    streaming_module.read_log_delta(
                        streaming_path,
                        0 if initial_load else cursor_start,
                        None
                        if initial_load
                        else CONSOLE_TRANSFER_BATCH_SIZE,
                    )
                )
        else:
            cursor_start = 0 if clear_consoles else int(
                execution_state["cursor"]
            )
            initial_load = True if clear_consoles else not execution_state[
                "initialized"
            ]
            log_lines, next_cursor, has_more = [], cursor_start, False
        execution_state["initialized"] = True
        execution_state["cursor"] = next_cursor
        if clear_consoles:
            execution_state["line_number"] = 0
        else:
            execution_state["line_number"] += len(log_lines)
        log_display_lines = []
        for persisted_sequence, line in log_lines:
            if persisted_sequence is None:
                persisted_sequence = execution_state["line_number"] - len(
                    log_lines
                ) + len(log_display_lines) + 1
            else:
                execution_state["persisted"] = True
            log_display_lines.append(
                f"[#{persisted_sequence}] "
                f"{format_presentation_log_line(line)}"
            )
        st.caption(
            f"Entradas recibidas: {execution_state['line_number']}"
        )
        render_log_console(
            log_display_lines,
            "execution-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
        )
    with alpaca_column:
        st.subheader("Alpaca Streaming")
        alpaca_state = get_console_state(
            "alpaca-log-console",
            run_id,
        )
        initial_load = not alpaca_state["initialized"]
        if streaming_path is not None:
            if clear_consoles:
                events, next_cursor, has_more = [], 0, False
                initial_load = True
            else:
                events, next_cursor, has_more = (
                    streaming_module.read_event_delta(
                        streaming_path,
                        0 if initial_load else int(alpaca_state["cursor"]),
                        None
                        if initial_load
                        else CONSOLE_TRANSFER_BATCH_SIZE,
                    )
                )
        else:
            next_cursor = 0 if clear_consoles else alpaca_state["cursor"]
            events, has_more = [], False
            initial_load = True if clear_consoles else initial_load
        alpaca_state["initialized"] = True
        event_lines = []
        for event in events:
            if event.console_sequence is None:
                event_sequence = alpaca_state["line_number"] + len(
                    event_lines
                ) + 1
            else:
                event_sequence = event.console_sequence
                alpaca_state["persisted"] = True
            event_lines.append(
                f"[#{event_sequence}] {event.to_display_line()}"
            )
        cursor_start = int(alpaca_state["cursor"])
        alpaca_state["cursor"] = next_cursor
        if clear_consoles:
            alpaca_state["line_number"] = 0
            cursor_start = 0
        else:
            alpaca_state["line_number"] += len(event_lines)
        st.caption(f"Eventos recibidos: {alpaca_state['line_number']}")
        render_log_console(
            event_lines,
            "alpaca-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
        )
        if not event_lines and status == "Conectado":
            st.info(
                "Conexión activa; todavía no se recibieron operaciones. "
                "El mercado puede estar cerrado."
            )

    with spark_column:
        st.subheader(
            "Apache Spark" + " - " +
            "[Spark Master ↗](http://localhost:8080/)"
        )
        spark_state = get_console_state("spark-log-console", run_id)
        initial_load = not spark_state["initialized"]
        if snapshot.get("console_schema_version", 0) >= 2:
            cursor_start = int(spark_state["cursor"])
            if clear_consoles:
                spark_records, next_cursor, has_more, schema_found = (
                    [], 0, False, False
                )
                cursor_start, initial_load = 0, True
            else:
                spark_records, next_cursor, has_more, schema_found = (
                    streaming_module.read_spark_delta(
                        streaming_path,
                        0 if initial_load else cursor_start,
                        None
                        if initial_load
                        else CONSOLE_TRANSFER_BATCH_SIZE,
                    )
                    if streaming_path is not None
                    else ([], cursor_start, False, False)
                )
            spark_state["cursor"] = next_cursor
            if clear_consoles:
                spark_state["line_number"] = 0
            else:
                spark_state["line_number"] += len(spark_records)
            spark_state["persisted"] = (
                spark_state["persisted"] or schema_found
            )
            spark_lines = [
                f"[#{sequence}] {line}"
                for sequence, line in spark_records
            ]
            spark_cursor_end = next_cursor
            spark_state["initialized"] = True
        else:
            spark_signature = () if clear_consoles else tuple(
                spark_console_lines
            )
            if spark_signature == spark_state.get("signature"):
                spark_lines = []
            else:
                spark_lines = spark_console_lines
                spark_state["signature"] = spark_signature
            cursor_start = int(spark_state["cursor"])
            spark_cursor_end = cursor_start + len(spark_lines)
            spark_state["cursor"] = spark_cursor_end
            if clear_consoles:
                spark_state["line_number"] = 0
            else:
                spark_state["line_number"] += len(spark_lines)
            has_more = False
            spark_state["initialized"] = True
        st.caption(
            f"Eventos recibidos: {spark_state['line_number']}"
        )
        render_log_console(
            spark_lines,
            "spark-log-console",
            cursor_end=spark_cursor_end,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
        )
    if clear_consoles:
        st.session_state.start_requested = False

    st.divider()
    st.subheader("Top 10 acciones con mayor volumen negociado")

    top_volume_rows = spark_result.get("top_volume", [])
    if not top_volume_rows:
        st.info("Todavía no hay volumen confirmado por Apache Spark.")
    else:
        render_volume_chart(top_volume_rows)

render_streaming_fragment = st.fragment(render_streaming, run_every="5s")
render_streaming_fragment()
