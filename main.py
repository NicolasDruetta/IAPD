"""Interfaz principal de Apache Big Data Lab."""

import importlib.util
import streamlit as st
import sys
import threading

from datetime import datetime, timezone, tzinfo
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

ALL_SYMBOLS_OPTION = "Todos los símbolos del S&P 500"
BIG_7SEVEN_SYMBOLS = ("AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA")
BIG_SEVEN_OPTION = "Big Seven del S&P 500: " + ", ".join(BIG_7SEVEN_SYMBOLS)

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
            } else if (!data.placeholder && lines.length > 0) {
                const separator = consoleElement.value.length > 0 ? "\\n" : "";
                const suffix = separator + lines.join("\\n");
                consoleElement.value += suffix;
            }
            const visibleLines = consoleElement.value.split("\\n");
            if (visibleLines.length > 1000) {
                consoleElement.value = visibleLines.slice(-1000).join("\\n");
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
TOP_LIQUID_OPTION = "Top 20 más líquidas del S&P 500"
TOP_VOLUME_OPTION = "Top 20 con más volumen del S&P 500"
SYMBOL_GROUPS = {
    ALL_SYMBOLS_OPTION: ("*",),
    BIG_SEVEN_OPTION: BIG_7SEVEN_SYMBOLS,
    TOP_LIQUID_OPTION: (),
    TOP_VOLUME_OPTION: (),
}
SYMBOL_OPTIONS = (
    ALL_SYMBOLS_OPTION,
    BIG_SEVEN_OPTION,
    TOP_LIQUID_OPTION,
    TOP_VOLUME_OPTION,
)
SYMBOL_RANKINGS = {
    TOP_LIQUID_OPTION: "trades",
    TOP_VOLUME_OPTION: "volume",
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
    if "start_operation" not in st.session_state:
        st.session_state.start_operation = None
    if "stop_pending" not in st.session_state:
        st.session_state.stop_pending = False
    if "console_states" not in st.session_state:
        st.session_state.console_states = {}
    if "rendered_run_active" not in st.session_state:
        st.session_state.rendered_run_active = False

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
            controller_module = sys.modules.get("01_controller")
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
        "placeholder": placeholder,
        "reset": initial_load and cursor_start == 0,
        "run_key": run_key,
    }
    LOG_CONSOLE_COMPONENT(
        data=payload,
        default={"need_more": None},
        height=420,
        key=console_id,
        on_need_more_change=lambda: None,
    )

def render_console_download_button(
    console_path: Path | None,
    console_id: str,
    run_id: str,
) -> None:
    """Muestro el botón pequeño para descargar el TXT completo."""

    default_names = {
        "alpaca-log-console": "log_02_alpaca_stream.txt",
        "execution-log-console": "log_01_controller.txt",
        "spark-log-console": "log_03_apache_spark.txt",
    }
    file_exists = console_path is not None and console_path.exists()
    file_name = (
        console_path.name
        if console_path is not None
        else default_names[console_id]
    )
    st.download_button(
        "↓",
        data=console_path.read_bytes() if file_exists else b"",
        file_name=file_name,
        mime="text/plain",
        key=f"download-{console_id}-{run_id}",
        help="Descargar log completo",
        disabled=not file_exists,
        type="secondary",
        width="content",
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
    dark_mode = bool(st.session_state.get("dark_mode", False))
    chart_border_color = "#3b404d" if dark_mode else "#dfe3eb"
    chart_text_color = "#ffffff" if dark_mode else "#2f3342"
    volume_encoding = {
        "field": "volume",
        "title": "Volumen negociado (acciones)",
        "type": "quantitative",
    }
    if not chart_rows:
        volume_encoding["axis"] = {"values": [0, 1]}
        volume_encoding["scale"] = {"domain": [0, 1]}
    chart_spec = {
        "background": "transparent",
        "data": {"values": chart_rows},
        "height": 360,
        "config": {
            "axis": {
                "domainColor": chart_border_color,
                "gridColor": chart_border_color,
                "labelColor": chart_text_color,
                "titleColor": chart_text_color,
                "tickColor": chart_border_color,
            },
            "text": {"color": chart_text_color},
            "view": {"stroke": chart_border_color},
        },
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
                        "title": "Acción",
                    },
                    "x": {
                        **volume_encoding,
                    },
                },
            },
            {
                "mark": {
                    "type": "text",
                    "align": "left",
                    "color": chart_text_color,
                    "dx": 4,
                },
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
        [0.8, 5.0, 0.5],
        gap="small",
        vertical_alignment="center",
    )
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
            width="content",
        ):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()
st.subheader(
    "Análisis de operaciones bursátiles con procesamiento distribuido en Apache Spark."
)
st.caption("Materia: IAPD. Autor: Nicolás Druetta. Año: 2026.")
st.divider()

def render_streaming() -> None:
    """Muestro las consolas de Alpaca y el estado de Spark."""

    controller_module = load_page_module("01_controller")
    streaming_module = load_page_module("02_alpaca_stream")
    spark_module = load_page_module("03_apache_spark")
    controller_module.configure_streaming_module(streaming_module)
    snapshot = controller_module.get_run_snapshot()
    previous_run_active = st.session_state.rendered_run_active
    active_run_id = snapshot["run_id"] if snapshot["is_active"] else None
    session_run_id = st.session_state.alpaca_run_id
    if session_run_id is None and active_run_id is not None:
        st.session_state.alpaca_run_id = active_run_id
        session_run_id = active_run_id
    if session_run_id == active_run_id and active_run_id is not None:
        controller_module.register_heartbeat(active_run_id)
        snapshot = controller_module.get_run_snapshot()

    universe = snapshot.get("universe", {})
    default_option = ALL_SYMBOLS_OPTION
    if snapshot["is_active"] and universe.get("selection") == "manual":
        manual_symbols = [
            symbol
            for symbol in snapshot["symbols"]
            if symbol in BIG_7SEVEN_SYMBOLS
        ]
        if tuple(manual_symbols) == BIG_7SEVEN_SYMBOLS:
            default_option = SYMBOL_OPTIONS[1]
    st.subheader("Símbolos")
    symbols_col, actions_col = st.columns(
        [4, 8],
        gap="small",
        vertical_alignment="bottom",
    )
    with symbols_col:
        with st.container(key="card-symbols"):
            selected_option = st.selectbox(
                "Símbolos",
                options=SYMBOL_OPTIONS,
                index=SYMBOL_OPTIONS.index(default_option),
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
            if snapshot["is_active"] or st.session_state.stop_pending:
                stop_clicked = st.button(
                    "⟳ Deteniendo..."
                    if st.session_state.stop_pending
                    else "■ Detener streaming",
                    disabled=st.session_state.stop_pending,
                    key="stop-button",
                    on_click=mark_stop_pending,
                    width="content",
                )
    symbols = list(SYMBOL_GROUPS[selected_option])
    symbol_ranking = SYMBOL_RANKINGS.get(selected_option)
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
            st.error(
                "Completá ALPACA_API_KEY y ALPACA_SECRET_KEY en pages/02_alpaca_stream.py."
            )
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
                    st.error(str(operation["error"]))
                else:
                    service = operation["service"]
                    st.session_state.alpaca_run_id = service.get_snapshot()[
                        "run_id"
                    ]
                    controller_module.register_heartbeat(
                        st.session_state.alpaca_run_id
                    )
                    st.session_state.start_pending = False
                    st.session_state.start_operation = None
                    snapshot = controller_module.get_run_snapshot()

    if stop_clicked or st.session_state.stop_pending:
        st.session_state.stop_pending = False
        controller_module.stop_active_run("manual_stop")
        snapshot = controller_module.get_run_snapshot()

    if (
        previous_run_active
        and not snapshot["is_active"]
        and snapshot["run_id"] is not None
    ):
        st.session_state.console_states = {}

    elapsed_text = format_elapsed_time(
        snapshot["started_at"],
        snapshot["ended_at"],
    )
    market_timestamp = snapshot.get("market_timestamp")
    presentation_market_timestamp = format_presentation_timestamp(
        market_timestamp,
    )
    if presentation_market_timestamp == "-":
        market_date = snapshot.get("historical_date") or "-"
        market_time = "-"
    else:
        market_date, market_time = presentation_market_timestamp.split(" ", 1)
    status = snapshot["status"]
    status_text = "Listo para ejecutar."
    if st.session_state.start_pending:
        status_text = "Iniciando..."
    elif snapshot["is_active"]:
        status_text = f"Ejecutando... {elapsed_text}."
    mode_col, run_col, status_col = st.columns(
        [2, 2, 8],
        gap="small",
    )
    with mode_col:
        with st.container(border=True):
            st.caption("Modo")
            st.text(snapshot["mode"] if snapshot["is_active"] else "-")
    with run_col:
        run_id = snapshot["run_id"] if snapshot["is_active"] else None
        with st.container(border=True):
            st.caption("RUN")
            st.text(run_id if run_id else "-")
    with status_col:
        with st.container(border=True):
            st.caption("Estado")
            st.text(status_text)

    if snapshot["is_active"] and snapshot["error"]:
        st.error(snapshot["error"])

    st.divider()

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
            "Detenido": "Detenido.",
            "Error": "Error.",
        }.get(snapshot["status"], f"{snapshot['status']}.")
        last_execution_notice = f"Datos de la **última ejecución:** {last_execution_text}. **Duración:** {elapsed_text}. **{completion_text}** | **Modo:** {history_mode}. | **RUN:** {history_run_id}."
        st.info(last_execution_notice)
        if snapshot["error"]:
            st.error(snapshot["error"])

    run_id = snapshot["run_id"]
    if clear_consoles and not snapshot["is_active"]:
        run_id = None
    spark_checkpoint = controller_module.get_spark_checkpoint(run_id)
    processed_events = int(
        (spark_checkpoint or {}).get("processed_events", 0)
    )
    spark_events = controller_module.get_events_after(
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
            "{symbol} | operaciones={operations} | cotizaciones={quotes} | volumen={volume}".format(
                symbol=row["symbol"],
                operations=row["operations"],
                quotes=row.get("quotes", 0),
                volume=row["total_volume"] or 0,
            )
            for row in spark_result["rows"]
        )
    if spark_result.get("checkpoint") is not None:
        controller_module.append_spark_checkpoint(
            run_id,
            spark_result["checkpoint"],
            spark_console_lines,
        )
    else:
        controller_module.ensure_spark_console_records(
            run_id,
            spark_console_lines,
        )
    console_header_columns = st.columns(
        3,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with console_header_columns[0]:
        st.subheader("1. Log del controlador")
    with console_header_columns[1]:
        st.subheader("2. Log de descarga de datos (Alpaca)")
    with console_header_columns[2]:
        st.subheader("3. Log de procesamiento (Spark)")
        st.write("[Spark Master ↗](http://localhost:8080/)")
    execution_column, alpaca_column, spark_column = st.columns(
        3,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with execution_column:
        execution_state = get_console_state(
            "execution-log-console",
            run_id,
        )
        streaming_file = snapshot.get("streaming_file")
        streaming_path = (
            (
                streaming_module.DATA_DIRECTORY
                if snapshot.get("console_schema_version", 0) >= 3
                else streaming_module.LEGACY_STREAMING_DIRECTORY
            ) / streaming_file
            if streaming_file
            else None
        )
        execution_path = (
            streaming_module.get_run_file(run_id, "handler")
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        alpaca_path = (
            streaming_module.get_run_file(run_id, "alpaca")
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        spark_path = (
            streaming_module.get_run_file(run_id, "spark")
            if snapshot.get("console_schema_version", 0) >= 3
            and run_id
            else streaming_path
        )
        if execution_path is not None:
            if clear_consoles:
                cursor_start, next_cursor, has_more = 0, 0, False
                initial_load, log_lines = True, []
            else:
                cursor_start = int(execution_state["cursor"])
                initial_load = not execution_state["initialized"]
                log_lines, next_cursor, has_more = (
                    streaming_module.read_console_delta(
                        execution_path,
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
        counter_col, download_col = st.columns([10, 1], gap="small")
        with counter_col:
            st.caption(
                f"Eventos recibidos: {execution_state['line_number']}"
            )
        with download_col:
            with st.container(horizontal=True, horizontal_alignment="right"):
                render_console_download_button(
                    streaming_module.get_run_file(run_id, "handler")
                    if run_id is not None else None,
                    "execution-log-console",
                    run_id or "no-run",
                )
        render_log_console(
            log_display_lines,
            "execution-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
            placeholder=is_placeholder,
        )
    with alpaca_column:
        alpaca_state = get_console_state(
            "alpaca-log-console",
            run_id,
        )
        initial_load = not alpaca_state["initialized"]
        if alpaca_path is not None:
            if clear_consoles:
                events, next_cursor, has_more = [], 0, False
                initial_load = True
            else:
                events, next_cursor, has_more = (
                    streaming_module.read_console_delta(
                        alpaca_path,
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
        event_lines = [
            format_presentation_log_line(line)
            for line in events
        ]
        cursor_start = int(alpaca_state["cursor"])
        alpaca_state["cursor"] = next_cursor
        if clear_consoles:
            alpaca_state["line_number"] = 0
            cursor_start = 0
        else:
            alpaca_state["line_number"] += len(event_lines)
        has_event_lines = bool(event_lines)
        is_placeholder = (
            not event_lines
            and alpaca_state["line_number"] == 0
        )
        if is_placeholder:
            event_lines = [
                "Alpaca | esperando cotizaciones u operaciones."
            ]
        counter_col, download_col = st.columns([10, 1], gap="small")
        with counter_col:
            st.caption(f"Eventos recibidos: {alpaca_state['line_number']}")
        with download_col:
            with st.container(horizontal=True, horizontal_alignment="right"):
                render_console_download_button(
                    streaming_module.get_run_file(run_id, "alpaca")
                    if run_id is not None else None,
                    "alpaca-log-console",
                    run_id or "no-run",
                )
        render_log_console(
            event_lines,
            "alpaca-log-console",
            cursor_end=next_cursor,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
            placeholder=is_placeholder,
        )
        if not has_event_lines and status == "Conectado":
            st.info(
                "Conexión activa; todavía no se recibieron cotizaciones u operaciones. El mercado puede estar cerrado."
            )

    with spark_column:
        spark_state = get_console_state("spark-log-console", run_id)
        initial_load = not spark_state["initialized"]
        cursor_start = int(spark_state["cursor"])
        if clear_consoles:
            spark_records, next_cursor, has_more = [], 0, False
            cursor_start, initial_load = 0, True
        else:
            spark_records, next_cursor, has_more = (
                streaming_module.read_console_delta(
                    spark_path,
                    0 if initial_load else cursor_start,
                    None
                    if initial_load
                    else CONSOLE_TRANSFER_BATCH_SIZE,
                )
                if spark_path is not None
                else ([], cursor_start, False)
            )
        spark_state["cursor"] = next_cursor
        if clear_consoles:
            spark_state["line_number"] = 0
        else:
            spark_state["line_number"] += len(spark_records)
        spark_lines = [
            format_presentation_log_line(line)
            for line in spark_records
        ]
        spark_cursor_end = next_cursor
        spark_state["initialized"] = True
        is_placeholder = not spark_lines and spark_state["line_number"] == 0
        if is_placeholder:
            spark_lines = ["Spark | esperando eventos bursátiles."]
        counter_col, download_col = st.columns([10, 1], gap="small")
        with counter_col:
            st.caption(f"Eventos recibidos: {spark_state['line_number']}")
        with download_col:
            with st.container(horizontal=True, horizontal_alignment="right"):
                render_console_download_button(
                    streaming_module.get_run_file(run_id, "spark")
                    if run_id is not None else None,
                    "spark-log-console",
                    run_id or "no-run",
                )
        render_log_console(
            spark_lines,
            "spark-log-console",
            cursor_end=spark_cursor_end,
            cursor_start=cursor_start,
            has_more=has_more,
            initial_load=initial_load,
            placeholder=is_placeholder,
        )
    if clear_consoles:
        st.session_state.start_requested = False

    st.divider()
    if last_execution_notice:
        st.info(last_execution_notice)
    with st.container(border=True):
        date_col, time_col, elapsed_col = st.columns(3, gap="small")
        with date_col:
            st.caption("Fecha analizada")
            st.text(market_date)
        with time_col:
            st.caption("Última hora procesada")
            st.text(market_time)
        with elapsed_col:
            st.caption("Tiempo transcurrido de ejecución")
            st.text(elapsed_text)
    st.subheader("Top 10 de acciones por volumen negociado")

    render_volume_chart(spark_result.get("top_volume", []))
    st.session_state.rendered_run_active = snapshot["is_active"]

render_streaming_fragment = st.fragment(render_streaming, run_every="5s")
render_streaming_fragment()
