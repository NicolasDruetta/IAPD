# Autor: Nicolás Druetta

import importlib.util
import math
import os
import signal
import streamlit as st
import sys
import threading

from app import database, functions
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

### Configuraciones.

DATA_DIRECTORY = Path(__file__).parent.parent / "data"
PRESENTATION_TIMEZONE = ZoneInfo("America/Argentina/Cordoba")
SHOW_STOP_DEBUG = False
SYMBOL_OPTIONS = {
    "Todas los empresas del S&P 500": {
        "value": ("*",),
        "ranking": None,
    },
    "Las 7 magníficas: AAPL, AMZN, GOOGL, META, MSFT, NVDA, TSLA": {
        "value": ("AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA"),
        "ranking": None,
    },
    "Top 20 empresas con más liquidez del S&P 500": {
        "value": (),
        "ranking": "trades",
    },
    "Top 20 empresas con más volumen del S&P 500": {
        "value": (),
        "ranking": "volume",
    },
}

### Componentes.

CONSOLE_ARROW_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.console_arrow",
    html=(
        '<span class="console-arrow-track">'
        '<span class="console-arrow" '
        'aria-hidden="true">📄→</span></span>'
    ),
    isolate_styles=False,
    js="""
    export default function ({ parentElement }) {
        const icon = parentElement.querySelector(".console-arrow");
        if (!icon) return () => {};
        const styles = getComputedStyle(icon);
        const duration = parseFloat(styles.animationDuration) || 4;
        const phase = (Date.now() / 1000) % duration;
        icon.style.animationDelay = `-${phase}s`;
        return () => {};
    }
    """,
)

EXECUTION_TIMER_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.execution_timer",
    html=(
        '<span class="execution-timer"></span>'
        '<span class="execution-heartbeat"></span>'
        '<div class="execution-heartbeat-debug"></div>'
    ),
    isolate_styles=False,
    js="""
    export default function ({ parentElement, data }) {
        const timerElement = parentElement.querySelector(".execution-timer");
        const heartbeatElement = parentElement.querySelector(
            ".execution-heartbeat",
        );
        const heartbeatDebugElement = parentElement.querySelector(
            ".execution-heartbeat-debug",
        );
        const state = parentElement.__executionTimerState || {
            interval: null,
            timerKey: null,
        };
        const formatElapsed = (startedAt, endedAt, active) => {
            const started = Date.parse(startedAt || "");
            if (Number.isNaN(started)) {
                return "00:00:00";
            }
            const end = active ? Date.now() : Date.parse(endedAt || "");
            const elapsed = Math.max(
                0,
                Math.floor(((Number.isNaN(end) ? Date.now() : end) - started) / 1000),
            );
            const hours = String(Math.floor(elapsed / 3600)).padStart(2, "0");
            const minutes = String(Math.floor((elapsed % 3600) / 60)).padStart(2, "0");
            const seconds = String(elapsed % 60).padStart(2, "0");
            return `${hours}:${minutes}:${seconds}`;
        };
        const render = () => {
            const elapsed = formatElapsed(
                data.started_at,
                data.ended_at,
                data.active,
            );
            timerElement.textContent = data.active
                ? data.prefix
                    ? `${data.prefix} ${elapsed}.`
                    : elapsed
                : data.text;
            if (heartbeatElement) {
                heartbeatElement.textContent = data.active && data.heartbeat_text
                    ? ` (Último heartbeat: ${data.heartbeat_text}).`
                    : "";
            }
            if (heartbeatDebugElement) {
                heartbeatDebugElement.textContent = data.active && data.heartbeat_debug
                    ? ` ${data.heartbeat_debug}`
                    : "";
            }
        };
        if (
            state.timerKey !== data.timer_key
            || (!data.active && state.interval !== null)
        ) {
            if (state.interval !== null) {
                clearInterval(state.interval);
            }
            state.timerKey = data.timer_key;
            state.interval = data.active ? setInterval(render, 1000) : null;
        }
        render();
        parentElement.__executionTimerState = state;
        return () => {};
    }
    """,
)

HEARTBEAT_REFRESH_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.heartbeat_refresh",
    html='<span aria-hidden="true"></span>',
    isolate_styles=False,
    js="""
    export default function ({ data }) {
        const heartbeat = document.querySelector(".execution-heartbeat");
        const debug = document.querySelector(".execution-heartbeat-debug");
        if (heartbeat === null) return () => {};
        heartbeat.textContent = data.active && data.text
            ? ` (Último heartbeat: ${data.text}).`
            : "";
        if (debug !== null) {
            debug.textContent = data.active && data.debug
                ? ` ${data.debug}`
                : "";
        }
        return () => {};
    }
    """,
)

LIVE_INDICATOR_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.live_indicator",
    html=(
        '<span class="live-indicator-anchor" aria-hidden="true"></span>'
    ),
    isolate_styles=False,
    js="""
    export default function ({ data }) {
        let cancelled = false;

        const update = () => {
            if (cancelled) return;
            const headings = Array.from(
                document.querySelectorAll("h1, h2, h3, h4, h5, h6"),
            );
            const heading = headings.find(
                (element) => element.textContent.trim().startsWith(data.title),
            );
            if (heading === undefined) return;
            let indicator = heading.querySelector(":scope > .live-indicator");
            if (indicator === null) {
                const liveIndicator = document.createElement("span");
                liveIndicator.className = "live-indicator";
                liveIndicator.innerHTML =
                    '<span class="live-dot" aria-hidden="true"></span>' +
                    '<span>LIVE</span>';
                heading.appendChild(liveIndicator);
                indicator = liveIndicator;
            }
            indicator.classList.toggle("is-hidden", !data.active);
        };

        update();
        const retryTimer = setInterval(update, 50);
        const stopRetry = setTimeout(() => clearInterval(retryTimer), 2000);
        return () => {
            cancelled = true;
            clearInterval(retryTimer);
            clearTimeout(stopRetry);
        };
    }
    """,
)

LOG_CONSOLE_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.log_console",
    html=(
        '<textarea class="log-console" readonly '
        'aria-label="Consola"></textarea>'
    ),
    isolate_styles=False,
    js="""
    export default function ({ parentElement, data }) {
        const consoleElement = parentElement.querySelector("textarea");
        const text = (Array.isArray(data.lines) ? data.lines : [])
            .slice(-200).join("\\n");
        if (consoleElement.value !== text) {
            consoleElement.value = text;
            consoleElement.scrollTop = consoleElement.scrollHeight;
            requestAnimationFrame(() => {
                consoleElement.scrollTop = consoleElement.scrollHeight;
            });
        }
        return () => {};
    }
    """,
)

SYSTEM_THEME_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.system_theme",
    html='<span class="system-theme-detector"></span>',
    isolate_styles=False,
    js="""
    export default function ({ parentElement, setStateValue }) {
        const state = parentElement.__systemThemeState || {
            mediaQuery: null,
            reportedTheme: null,
        };
        if (state.mediaQuery === null) {
            state.mediaQuery = window.matchMedia(
                "(prefers-color-scheme: dark)",
            );
            state.mediaQuery.addEventListener("change", (event) => {
                state.reportedTheme = event.matches;
                setStateValue("dark", event.matches);
            });
        }
        if (state.reportedTheme !== state.mediaQuery.matches) {
            state.reportedTheme = state.mediaQuery.matches;
            setStateValue("dark", state.reportedTheme);
        }
        parentElement.__systemThemeState = state;
        return () => {};
    }
    """,
)

### Renders primarios (main).

def load_custom_css() -> None:
    """Cargo los estilos visuales administrados en el archivo del proyecto."""

    css_path = Path(__file__).parent.parent / "assets" / "custom.css"
    css_content = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

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
    if "stop_operation" not in st.session_state:
        st.session_state.stop_operation = None
    if "manual_stop_status" not in st.session_state:
        st.session_state.manual_stop_status = False
    if "last_stop_debug" not in st.session_state:
        st.session_state.last_stop_debug = []
    if "console_states" not in st.session_state:
        st.session_state.console_states = {}

def render_header() -> None:
    """Muestro el encabezado general del laboratorio."""

    st.markdown('<div id="top"></div>', unsafe_allow_html=True)
    icon_path = Path(__file__).parent.parent / "assets" / "icon.png"
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

    render_heartbeat()

    st.subheader("Procesamiento distribuido y análisis estadístico de datos bursátiles.")
    st.write("**Pipeline:** Controller → Alpaca → Memoria → Kafka → Streaming → Spark → Parquet → Pandas → PostgreSQL → Streamlit.")
    st.caption("Materia: Procesamiento de Datos, TP2. Autor: Nicolás Druetta. Año: 2026.")

def render_config() -> None:
    """Muestro la configuración y coordino el estado del RUN."""

    controller_module = load_page_module("01_python_controller")
    streaming_module = load_page_module("02_alpaca_stream")
    kafka_module = load_page_module("03_apache_kafka")
    spark_module = load_page_module("04_apache_spark")
    load_page_module("05_pandas")
    load_page_module("06_postgresql")
    controller_module.configure_streaming_module(streaming_module)
    controller_module.configure_pipeline_modules(
        kafka_module,
        spark_module,
    )
    snapshot = controller_module.get_run_snapshot()
    active_run_id = snapshot["run_id"] if snapshot["is_active"] else None
    if st.session_state.alpaca_run_id is None and active_run_id is not None:
        st.session_state.alpaca_run_id = active_run_id
    pending_stop_operation = st.session_state.stop_operation
    if (
        st.session_state.stop_pending
        and (
            pending_stop_operation is None
            or pending_stop_operation["done"]
            or (
                not snapshot["is_active"]
                and not spark_module.has_active_query()
            )
        )
        and not snapshot["is_active"]
        and not spark_module.has_active_query()
    ):
        st.session_state.last_stop_debug = list(
            pending_stop_operation.get("debug", [])
            if pending_stop_operation is not None
            else []
        )
        st.session_state.stop_pending = False
        st.session_state.stop_operation = None
        st.session_state.start_requested = False
        st.session_state.alpaca_run_id = None
        st.rerun()

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

    st.write(f"**Símbolos bursátiles a analizar:**")
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
                    or st.session_state.stop_pending
                ),
                key="start-button",
                on_click=mark_start_pending,
                width="content",
            )
            stop_placeholder = st.empty()
            if (
                snapshot["is_active"] or st.session_state.stop_pending
            ) and not st.session_state.start_pending:
                stop_clicked = stop_placeholder.button(
                    "⟳ Deteniendo..."
                    if st.session_state.stop_pending
                    else "■ Detener streaming",
                    disabled=st.session_state.stop_pending,
                    key="stop-button",
                    on_click=mark_stop_pending,
                    width="content",
                )
            else:
                stop_placeholder.empty()
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
            st.error(
                "No se puede iniciar el RUN porque faltan las credenciales "
                "de Alpaca. Verifique ALPACA_API_KEY y ALPACA_SECRET_KEY en "
                ".env y vuelva a iniciar el contenedor."
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
                else:
                    service = operation["service"]
                    run_id = service.get_snapshot()["run_id"]
                    try:
                        st.session_state.alpaca_run_id = run_id
                        register_heartbeat_for_display(controller_module, run_id)
                        controller_module.start_pipeline(run_id)
                        st.session_state.controller_log_animation_run_id = run_id
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

    if stop_clicked or (
        st.session_state.stop_pending
        and st.session_state.stop_operation is None
    ):
        st.session_state.manual_stop_status = True
        st.session_state.start_requested = False
        clear_consoles = False
        run_id = snapshot.get("run_id")
        operation = {
            "done": False,
            "debug": ["STOP recibido: iniciando detención."],
            "error": None,
            "spark_result": None,
        }
        stop_thread = threading.Thread(
            target=stop_run_in_background,
            args=(
                operation,
                controller_module,
                spark_module,
                run_id,
            ),
            daemon=True,
            name="stop-streaming-run",
        )
        operation["thread"] = stop_thread
        st.session_state.stop_operation = operation
        st.session_state.stop_pending = True
        stop_thread.start()

    stop_operation = st.session_state.stop_operation
    stop_completed = False
    stop_debug = (
        stop_operation.get("debug", [])
        if stop_operation is not None
        else st.session_state.last_stop_debug
        if st.session_state.manual_stop_status
        else []
    )
    if SHOW_STOP_DEBUG and stop_debug:
        st.warning(
            "**STOP DEBUG**  |  "
            + "  |  ".join(stop_debug)
        )
    if (
        st.session_state.stop_pending
        and stop_operation is not None
        and (
            stop_operation["done"]
            or (
                not snapshot["is_active"]
                and not spark_module.has_active_query()
            )
        )
    ):
        st.session_state.last_stop_debug = list(
            stop_operation.get("debug", [])
        )
        st.session_state.stop_pending = False
        st.session_state.stop_operation = None
        st.session_state.start_requested = False
        st.session_state.alpaca_run_id = None
        snapshot = controller_module.get_run_snapshot()
        stop_completed = True

    if stop_completed:
        st.rerun()

    if st.session_state.start_pending:
        elapsed_text = "-"
        market_date = "El último día hábil completo."
        market_time = "-"
        heartbeat_text = "-"
        heartbeat_debug = "-"
    else:
        elapsed_text = format_elapsed_time(
            snapshot["started_at"],
            snapshot["ended_at"],
        )
        market_timestamp = snapshot.get("market_timestamp")
        presentation_market_timestamp = format_presentation_timestamp(
            market_timestamp,
        )
        current_run = st.session_state.alpaca_run_id
        market_date = (
            snapshot.get("historical_date") or "El último día hábil completo."
            if current_run is not None or snapshot["is_active"]
            else "El último día hábil completo."
        )
        market_time = (
            presentation_market_timestamp.split(" ", 1)[1]
            if presentation_market_timestamp != "-"
            else "-"
        )
        heartbeat_text = format_presentation_timestamp(
            snapshot.get("last_heartbeat"),
        ).split()[-1]
        heartbeat_debug = (
            f"Heartbeat ID: {snapshot.get('heartbeat_id', '-')} | "
            f"Sesión actual: {controller_module.get_heartbeat_session_id()} | "
            f"Última sesión: {snapshot.get('last_heartbeat_session_id') or '-'} | "
            f"Heartbeats: {snapshot.get('heartbeat_count', 0)}"
        )
    status_text = "Listo para ejecutar."
    if st.session_state.stop_pending:
        status_text = "Deteniendo..."
    elif st.session_state.manual_stop_status:
        status_text = "Detenido manualmente."
    elif st.session_state.start_pending:
        status_text = "Iniciando..."
    elif snapshot["is_active"]:
        status_text = f"Ejecutando... {elapsed_text}."
    elif (
        st.session_state.alpaca_run_id is not None
        and snapshot["status"] == "Error"
    ):
        status_text = "Finalizado con error."
    market_date_col, run_col, status_col = st.columns(
        [1, 1, 2],
        gap="small",
    )
    with market_date_col:
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
                EXECUTION_TIMER_COMPONENT(
                    data={
                        "active": True,
                        "ended_at": snapshot["ended_at"],
                        "started_at": snapshot["started_at"],
                        "text": status_text,
                        "timer_key": snapshot["run_id"],
                        "prefix": "Ejecutando...",
                        "heartbeat_text": heartbeat_text,
                        "heartbeat_debug": heartbeat_debug,
                    },
                    key=f"execution-timer-{heartbeat_text}",
                )
            else:
                st.text(status_text)

    error_placeholder = st.empty()
    error_text = " ".join(str(snapshot.get("error") or "").split())
    if (
        error_text
        and not snapshot["is_active"]
        and not st.session_state.start_pending
        and snapshot.get("end_reason") != "manual_stop"
    ):
        error_placeholder.error(
            "**Error:** " + error_text
        )

    last_execution_notice = get_last_execution_notice(snapshot)
    automatic_stop_messages = {
        "heartbeat_timeout": f"La ejecución se detuvo automáticamente porque la interfaz dejó de enviar señales de actividad durante {controller_module.HEARTBEAT_TIMEOUT_MINUTES} minutos.",
        "process_restart": "La ejecución se detuvo automáticamente porque se reinició el proceso de la aplicación.",
        "process_shutdown": "La ejecución se detuvo automáticamente porque se cerró el proceso de la aplicación.",
    }
    automatic_stop_reason = snapshot.get("end_reason")
    automatic_stop_message = automatic_stop_messages.get(
        automatic_stop_reason,
    )
    automatic_stop_placeholder = st.empty()
    if (
        automatic_stop_message
        and automatic_stop_reason != "manual_stop"
        and not snapshot["is_active"]
        and not st.session_state.start_pending
    ):
        automatic_stop_placeholder.error(automatic_stop_message)
    last_execution_placeholder = st.empty()
    if last_execution_notice:
        last_execution_placeholder.info(last_execution_notice)

    run_id = None if st.session_state.start_pending else snapshot["run_id"]
    if (
        run_id is not None
        and snapshot["is_active"]
        and not st.session_state.stop_pending
    ):
        try:
            controller_module.start_pipeline(run_id)
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

def render_consoles_part1() -> None:
    """Muestro las primeras cuatro consolas del pipeline."""

    _refresh_heartbeat_display("heartbeat-refresh-console")
    with st.container(key="consoles-part1"):
        execution_column, alpaca_column = st.columns(
            2,
            gap="small",
            vertical_alignment="top",
            wrap=True,
        )
        with execution_column:
            render_controller_console()
        with alpaca_column:
            render_alpaca_console()
        kafka_column, spark_column = st.columns(
            2,
            gap="small",
            vertical_alignment="top",
            wrap=True,
        )
        with kafka_column:
            render_kafka_console()
        with spark_column:
            render_spark_console()

def render_consoles_part2() -> None:
    """Muestro las consolas de análisis y persistencia del pipeline."""

    with st.container(key="consoles-part2"):
        pandas_column, postgresql_column = st.columns(
            2,
            gap="small",
            vertical_alignment="top",
            wrap=True,
        )
        with pandas_column:
            render_pandas_console()
        with postgresql_column:
            render_postgresql_console()

def render_info() -> None:
    """Muestro la información de la última ejecución."""

    render_live_indicator("Información", "live-info")
    controller_module = load_page_module("01_python_controller")
    streaming_module = load_page_module("02_alpaca_stream")
    controller_module.configure_streaming_module(streaming_module)
    snapshot = controller_module.get_run_snapshot()
    last_execution_notice = get_last_execution_notice(snapshot)
    if last_execution_notice:
        st.info(last_execution_notice)
    if st.session_state.start_pending:
        elapsed_text = "-"
        market_date = "-"
        market_time = "-"
        downloaded_count = "-"
    else:
        elapsed_text = format_elapsed_time(
            snapshot["started_at"],
            snapshot["ended_at"],
        )
        market_timestamp = snapshot.get("market_timestamp")
        presentation_market_timestamp = format_presentation_timestamp(
            market_timestamp,
        )
        market_date = snapshot.get("historical_date") or "-"
        market_time = (
            presentation_market_timestamp.split(" ", 1)[1]
            if presentation_market_timestamp != "-"
            else "-"
        )
        downloaded_count = get_console_state(
            "alpaca-log-console",
            snapshot.get("run_id"),
        )["line_number"]

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
                EXECUTION_TIMER_COMPONENT(
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
            st.text(str(downloaded_count))

def render_charts() -> None:
    """Muestro los gráficos de la última ejecución."""

    render_live_indicator("Gráficos", "live-charts")
    if st.session_state.start_requested or st.session_state.start_pending:
        analysis_data = {
            "activity_interval": 1,
            "activity_rows": [],
            "top_volume": [],
        }
    else:
        analysis_data = st.session_state.get(
            "analysis_data",
            {
                "activity_interval": 1,
                "activity_rows": [],
                "top_volume": [],
            },
        )

    top_volume = analysis_data.get("top_volume", [])
    activity_rows = analysis_data.get("activity_rows", [])
    activity_interval = analysis_data.get("activity_interval", 1)
    volume_column, activity_column = st.columns(
        2,
        gap="small",
        vertical_alignment="top",
    )
    with volume_column:
        st.write(f"**1. Top 10 acciones por volumen negociado.**")
        render_volume_chart(top_volume)
    with activity_column:
        st.write(f"**2. Actividad bursátil durante la sesión de mercado.**")
        render_activity_chart(activity_rows, activity_interval)

def render_questions() -> None:
    """Muestro las preguntas de investigación y sus tablas."""

    render_live_indicator("Preguntas de investigación", "live-questions",)
    if st.session_state.start_requested or st.session_state.start_pending:
        analysis_data = {
            "answers": [],
            "metrics": [],
        }
    else:
        analysis_run_id = st.session_state.alpaca_run_id
        if analysis_run_id is None:
            streaming_module = load_page_module("02_alpaca_stream")
            metadata = streaming_module.get_latest_metadata()
            analysis_run_id = metadata.get("run_id") if metadata else None
        analysis_data = database.get_market_analysis(analysis_run_id)
    st.session_state.analysis_data = analysis_data
    answers = {
        str(answer["question_id"]): answer
        for answer in analysis_data.get("answers", [])
    }
    metrics = analysis_data.get("metrics", [])
    question_columns = st.columns(
        2,
        gap="small",
        vertical_alignment="top",
    )
    with question_columns[0]:
        st.write(f"**1. ¿Las acciones con más operaciones presentan mayor volatilidad?**")
        volatility_answer = answers.get("volatility")
        volatility_correlation = (
            volatility_answer.get("correlation")
            if volatility_answer
            else None
        )
        volatility_rows = [
            {
                "symbol": row.get("symbol"),
                "trade_count": row.get("trade_count"),
                "return_count": row.get("return_count"),
                "volatility": row.get("volatility"),
            }
            for row in metrics
        ]
        st.dataframe(volatility_rows, hide_index=True, width="stretch")
        volatility_correlation_text = (
            "Datos insuficientes"
            if volatility_correlation is None
            else f"{float(volatility_correlation):.4f}"
        )
        volatility_conclusion = describe_correlation(
            volatility_correlation
            if volatility_answer and volatility_answer.get("symbol_count", 0) >= 2
            else None,
            "la cantidad de operaciones",
            "la volatilidad",
        )
        st.write(
            f"**Correlación:** {volatility_correlation_text} "
            "(cantidad de operaciones y volatilidad)."
        )
        st.write(f"**Interpretación:** {volatility_conclusion}")
    with question_columns[1]:
        st.write(f"**2. ¿Las acciones más negociadas tienen menor spread entre compra y venta?**")
        spread_answer = answers.get("spread")
        spread_correlation = (
            spread_answer.get("correlation")
            if spread_answer
            else None
        )
        spread_rows = [
            {
                "symbol": row.get("symbol"),
                "trade_count": row.get("trade_count"),
                "spread_count": row.get("spread_count"),
                "spread_mean": row.get("spread_mean"),
            }
            for row in metrics
        ]
        st.dataframe(spread_rows, hide_index=True, width="stretch")
        spread_correlation_text = (
            "Datos insuficientes"
            if spread_correlation is None
            else f"{float(spread_correlation):.4f}"
        )
        spread_conclusion = describe_correlation(
            spread_correlation
            if spread_answer and spread_answer.get("symbol_count", 0) >= 2
            else None,
            "la cantidad de operaciones",
            "el spread",
        )
        st.write(
            f"**Correlación:** {spread_correlation_text} "
            "(cantidad de operaciones y spread)."
        )
        st.write(f"**Interpretación:** {spread_conclusion}")

def render_footer() -> None:
    """Muestro el pie del laboratorio."""

    with st.container(key="footer"):
        st.caption("Materia: Procesamiento de Datos, TP2. Autor: Nicolás Druetta. Año: 2026.")
        st.markdown("[Volver arriba ↑](#top)")

### Renders secundarios (hijos).

def render_heartbeat() -> None:
    """Renuevo la actividad del RUN mientras el navegador mantiene la sesión."""

    _refresh_heartbeat_display("heartbeat-refresh-header", register=True)

def render_controller_console() -> None:
    """Muestro la consola del Controller."""

    clear_consoles, run_id, execution_path, _, _, _, _, _, _ = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("1. Log del controlador (Python)", width="content")
        render_live_indicator("1. Log del controlador (Python)", "live-controller-log")
    execution_state = get_console_state("execution-log-console", run_id)
    log_lines, _, total_lines, _, _ = get_console_buffer_view(
        run_id, "controller", execution_path, execution_state, clear_consoles
    )
    if clear_consoles:
        execution_state["display_line_number"] = 0
    elif "display_line_number" not in execution_state:
        execution_state["display_line_number"] = (
            0
            if st.session_state.get("controller_log_animation_run_id") == run_id
            else total_lines
        )
    if execution_state["display_line_number"] < total_lines:
        execution_state["display_line_number"] += 1
    log_display_lines = [
        format_presentation_log_line(line)
        for line in log_lines[:min(execution_state["display_line_number"], len(log_lines))]
    ]
    if not log_display_lines and execution_state["line_number"] == 0:
        log_display_lines = ["Python | esperando actividad."]
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(f"Eventos recibidos: **{execution_state['line_number']}**.")
        st.download_button(
            "↓",
            data=execution_path.read_bytes() if execution_path is not None and execution_path.exists() else b"",
            file_name=execution_path.name if execution_path is not None else "01_python_controller_log.txt",
            mime="text/plain",
            key=f"download-execution-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(log_display_lines, "execution-log-console")

def render_alpaca_console() -> None:
    """Muestro la consola de descarga de Alpaca."""

    clear_consoles, run_id, _, alpaca_path, _, _, _, _, is_active = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("2. Log de descarga (Alpaca)", width="content")
        if is_active and alpaca_path is not None and alpaca_path.exists() and alpaca_path.stat().st_size:
            CONSOLE_ARROW_COMPONENT(key="console-arrow-alpaca", width="stretch")
    alpaca_state = get_console_state("alpaca-log-console", run_id)
    events, _, _, _, _ = get_console_buffer_view(
        run_id, "alpaca", alpaca_path, alpaca_state, clear_consoles
    )
    event_lines = [format_presentation_log_line(line) for line in events]
    if not event_lines and alpaca_state["line_number"] == 0:
        event_lines = ["Alpaca | esperando cotizaciones y operaciones."]
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(f"Datos descargados: **{alpaca_state['line_number']}**.")
        st.download_button(
            "↓",
            data=alpaca_path.read_bytes() if alpaca_path is not None and alpaca_path.exists() else b"",
            file_name=alpaca_path.name if alpaca_path is not None else "02_alpaca_stream_log.txt",
            mime="text/plain",
            key=f"download-alpaca-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(event_lines, "alpaca-log-console")

def render_kafka_console() -> None:
    """Muestro la consola de publicación de Kafka."""

    clear_consoles, run_id, _, _, kafka_path, _, _, _, is_active = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("3. Log de publicación (Kafka)", width="content")
        if is_active and kafka_path is not None and kafka_path.exists() and kafka_path.stat().st_size:
            CONSOLE_ARROW_COMPONENT(key="console-arrow-kafka", width="stretch")
    kafka_state = get_console_state("kafka-log-console", run_id)
    kafka_records, _, _, _, _ = get_console_buffer_view(
        run_id, "kafka", kafka_path, kafka_state, clear_consoles
    )
    kafka_lines = [format_presentation_log_line(line) for line in kafka_records]
    if not kafka_lines and kafka_state["line_number"] == 0:
        kafka_lines = ["Kafka | esperando datos para publicar."]
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(
            f"Eventos recibidos: **{kafka_state['line_number']}**. "
            f"Datos procesados: **{functions.count_processed_data(kafka_path, 'kafka')}**."
        )
        st.download_button(
            "↓",
            data=kafka_path.read_bytes() if kafka_path is not None and kafka_path.exists() else b"",
            file_name=kafka_path.name if kafka_path is not None else "03_apache_kafka_log.txt",
            mime="text/plain",
            key=f"download-kafka-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(kafka_lines, "kafka-log-console")

def render_spark_console() -> None:
    """Muestro la consola de procesamiento de Spark."""

    clear_consoles, run_id, _, _, _, spark_path, _, _, is_active = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("4. Log de procesamiento (Spark)", width="content")
        if is_active and spark_path is not None and spark_path.exists() and spark_path.stat().st_size:
            CONSOLE_ARROW_COMPONENT(key="console-arrow-spark", width="stretch")
    spark_state = get_console_state("spark-log-console", run_id)
    spark_records, _, _, _, _ = get_console_buffer_view(
        run_id, "spark", spark_path, spark_state, clear_consoles
    )
    spark_lines = [format_presentation_log_line(line) for line in spark_records]
    if not spark_lines and spark_state["line_number"] == 0:
        spark_lines = ["Spark | esperando datos para procesar."]
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(
            f"Eventos recibidos: **{spark_state['line_number']}**. "
            f"Datos procesados: **{functions.count_processed_data(spark_path, 'spark')}**."
        )
        st.download_button(
            "↓",
            data=spark_path.read_bytes() if spark_path is not None and spark_path.exists() else b"",
            file_name=spark_path.name if spark_path is not None else "04_apache_spark_log.txt",
            mime="text/plain",
            key=f"download-spark-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(spark_lines, "spark-log-console")
    st.caption(
        "**[Spark Master ↗](http://localhost:8080/)** &nbsp;-&nbsp; "
        "**[Worker 1 ↗](http://localhost:8081/)** &nbsp;-&nbsp; "
        "**[Worker 2 ↗](http://localhost:8082/)** &nbsp;-&nbsp; "
        "**[Spark Jobs ↗](http://localhost:4040/)** "
    )

def render_pandas_console() -> None:
    """Muestro la consola de análisis de Pandas."""

    clear_consoles, run_id, _, _, _, _, pandas_path, _, is_active = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("5. Log de análisis (Pandas)", width="content")
        if is_active and pandas_path is not None and pandas_path.exists() and pandas_path.stat().st_size:
            CONSOLE_ARROW_COMPONENT(key="console-arrow-pandas", width="stretch")
    pandas_state = get_console_state("pandas-log-console", run_id)
    pandas_records, _, _, _, _ = get_console_buffer_view(
        run_id, "pandas", pandas_path, pandas_state, clear_consoles
    )
    pandas_lines = [format_presentation_log_line(line) for line in pandas_records]
    if not pandas_lines and pandas_state["line_number"] == 0:
        pandas_lines = ["Pandas | esperando datos para analizar."]
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(
            f"Eventos recibidos: **{pandas_state['line_number']}**. "
            f"Datos procesados: **{functions.count_processed_data(pandas_path, 'pandas')}**."
        )
        st.download_button(
            "↓",
            data=pandas_path.read_bytes() if pandas_path is not None and pandas_path.exists() else b"",
            file_name=pandas_path.name if pandas_path is not None else "05_pandas_log.txt",
            mime="text/plain",
            key=f"download-pandas-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(pandas_lines, "pandas-log-console")

def render_postgresql_console() -> None:
    """Muestro la consola de persistencia de PostgreSQL."""

    clear_consoles, run_id, _, _, _, _, _, postgresql_path, _ = _get_consoles_context()
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        st.subheader("6. Log de persistencia (PostgreSQL)", width="content")
    postgresql_state = get_console_state("postgresql-log-console", run_id)
    postgresql_records, _, _, _, _ = get_console_buffer_view(
        run_id, "postgresql", postgresql_path, postgresql_state, clear_consoles
    )
    postgresql_lines = [format_presentation_log_line(line) for line in postgresql_records]
    if not postgresql_lines and postgresql_state["line_number"] == 0:
        postgresql_lines = ["PostgreSQL | esperando datos para guardar en la base."]
    postgresql_counters = functions.get_postgresql_counters(postgresql_path)
    with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
        st.caption(
            f"Eventos recibidos: **{postgresql_counters['events']}**. "
            f"Registros nuevos: **{postgresql_counters['total']}** "
            f"({postgresql_counters['metrics']} métricas, {postgresql_counters['answers']} respuestas). "
            f"Actualizaciones realizadas: **{postgresql_counters['updates']}**."
        )
        st.download_button(
            "↓",
            data=postgresql_path.read_bytes() if postgresql_path is not None and postgresql_path.exists() else b"",
            file_name=postgresql_path.name if postgresql_path is not None else "06_postgresql_log.txt",
            mime="text/plain",
            key=f"download-postgresql-log-console-{run_id or 'no-run'}",
            help="Descargar log completo",
            type="secondary",
            width=36,
        )
    render_log_console(postgresql_lines, "postgresql-log-console")
    if clear_consoles:
        st.session_state.start_requested = False

### Renders terciarios (otros).

def describe_correlation(
    correlation: Any,
    first_variable: str,
    second_variable: str,
) -> str:
    """Construyo una interpretación específica en rangos de 0,10."""

    try:
        correlation = float(correlation)
    except (TypeError, ValueError):
        correlation = None
    if correlation is None or not math.isfinite(correlation):
        return "Datos insuficientes para establecer una correlación."
    absolute_correlation = abs(correlation)
    if absolute_correlation > 1:
        return "El valor de correlación obtenido no es válido."
    strengths = (
        (0.10, "prácticamente nula"),
        (0.20, "muy débil"),
        (0.30, "débil"),
        (0.40, "débil a moderada"),
        (0.50, "moderada"),
        (0.60, "moderada"),
        (0.70, "moderada a fuerte"),
        (0.80, "fuerte"),
        (0.90, "muy fuerte"),
        (1.01, "casi perfecta"),
    )
    strength = next(
        label
        for threshold, label in strengths
        if absolute_correlation < threshold
    )
    percentage = absolute_correlation * 100
    if absolute_correlation < 0.10:
        direction_text = "no muestra una tendencia lineal apreciable"
    elif correlation > 0:
        direction_text = (
            f"es positiva: cuando aumenta {first_variable}, "
            f"{second_variable} tiende a aumentar"
        )
    else:
        direction_text = (
            f"es negativa: cuando aumenta {first_variable}, "
            f"{second_variable} tiende a disminuir"
        )
    return (
        f"La relación entre {first_variable} y {second_variable} es "
        f"{strength} ({percentage:.1f}% de intensidad absoluta) y "
        f"{direction_text}."
    )

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

def get_console_state(console_id: str, run_id: str | None) -> dict[str, Any]:
    """Mantengo el contador visual independiente de cada consola y RUN."""

    key = run_id or "empty"
    state = st.session_state.console_states.setdefault(console_id, {})
    if state.get("run_key") != key:
        state.clear()
        state.update({"run_key": key, "line_number": 0})
    return state

def get_last_execution_notice(snapshot: dict[str, Any]) -> str | None:
    """Construyo el aviso visible de la última ejecución terminada."""

    if (
        not snapshot.get("run_id")
        or snapshot.get("is_active")
        or st.session_state.start_pending
        or st.session_state.stop_pending
    ):
        return None
    last_execution_at = snapshot.get("ended_at") or snapshot.get("started_at")
    last_execution_text = (
        format_presentation_timestamp(last_execution_at)
        if last_execution_at
        else "-"
    )
    elapsed_text = format_elapsed_time(
        snapshot.get("started_at"),
        snapshot.get("ended_at"),
    )
    history_market_date = snapshot.get("historical_date") or "-"
    history_run_id = snapshot.get("run_id") or "-"
    completion_text = {
        "Completado": "Finalizado correctamente.",
        "Error": "Finalizado con error.",
    }.get(snapshot.get("status"), f"{snapshot.get('status')}.")
    if (
        snapshot.get("status") == "Detenido"
        and snapshot.get("end_reason") == "manual_stop"
    ):
        completion_text = "Detenido manualmente."
    return f"Datos de la **última ejecución:** {last_execution_text}. **Duración:** {elapsed_text}. **{completion_text}** | **Fecha de mercado:** {history_market_date}. | **RUN ID:** {history_run_id}."

def handle_start_error(
    error: Exception,
    controller_module: ModuleType,
    spark_module: ModuleType,
    run_id: str | None,
) -> None:
    """Finalizo el RUN y conservo el error visible tras un fallo de inicio."""

    error_message = " ".join(str(error).split()) or error.__class__.__name__
    if "NoBrokersAvailable" in error_message:
        error_message = (
            "NoBrokersAvailable: El servicio kafka no está "
            "listo o no responde en kafka:9092. Espere a que Docker lo marque "
            "como saludable y vuelva a iniciar el RUN. No se procesaron datos."
        )
    if run_id is not None:
        spark_module.stop_streaming(run_id, finalize=False)
    controller_module.fail_active_run(error_message)
    st.session_state.start_pending = False
    st.session_state.start_operation = None
    st.session_state.alpaca_run_id = None

def load_page_module(module_name: str) -> ModuleType:
    """Cargo un módulo de procesamiento sin construir su interfaz."""

    module_path = Path(__file__).parent.parent / "pipeline" / f"{module_name}.py"
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

def mark_start_pending() -> None:
    """Marco que el usuario acaba de solicitar el inicio."""

    st.session_state.manual_stop_status = False
    st.session_state.start_requested = True
    st.session_state.start_pending = True
    st.rerun()

def mark_stop_pending() -> None:
    """Envío STOP al servicio antes de que Streamlit vuelva a renderizar."""

    st.session_state.stop_pending = True
    controller_module = load_page_module("01_python_controller")
    controller_module.request_stop_active_run("manual_stop")

def register_heartbeat_for_display(controller_module: Any, run_id: str) -> bool:
    """Registro el heartbeat y conservo su instante para mostrarlo en pantalla."""

    registered = controller_module.register_heartbeat(run_id)
    return registered

def render_activity_chart(
    rows: list[dict[str, Any]],
    interval_seconds: int,
) -> None:
    """Muestro la actividad bursátil con escala temporal adaptativa."""

    dark_mode = bool(st.session_state.get("dark_mode", False))
    chart_border_color = "#3b404d" if dark_mode else "#dfe3eb"
    chart_text_color = "#ffffff" if dark_mode else "#2f3342"
    events_encoding = {
        "field": "events",
        "title": "Cantidad de eventos",
        "type": "quantitative",
    }
    if not rows:
        events_encoding["axis"] = {"values": [0, 1]}
        events_encoding["scale"] = {"domain": [0, 1]}
    chart_spec = {
        "background": "transparent",
        "data": {"values": rows},
        "height": 360,
        "config": {
            "axis": {
                "domainColor": chart_border_color,
                "gridColor": chart_border_color,
                "labelColor": chart_text_color,
                "titleColor": chart_text_color,
                "tickColor": chart_border_color,
            },
            "legend": {
                "labelColor": chart_text_color,
                "titleColor": chart_text_color,
            },
            "view": {"stroke": chart_border_color},
        },
        "mark": {"point": True, "type": "line"},
        "encoding": {
            "color": {
                "field": "event_type",
                "title": "Tipo",
                "type": "nominal",
            },
            "x": {
                "axis": {
                    "format": (
                        "%H:%M:%S"
                        if interval_seconds < 60
                        else "%H:%M"
                    ),
                    "labelAngle": -45,
                    "labelOverlap": "greedy",
                },
                "field": "second",
                "title": "Fecha y hora bursátil",
                "type": "temporal",
            },
            "y": events_encoding,
        },
    }
    st.vega_lite_chart(
        spec=chart_spec,
        use_container_width=True,
    )

def render_live_indicator(title: str, key: str) -> None:
    """Muestro LIVE únicamente durante una ejecución activa."""

    controller_module = load_page_module("01_python_controller")
    snapshot = controller_module.get_run_snapshot()
    is_active = bool(
        snapshot.get("is_active")
        and not st.session_state.start_pending
        and not st.session_state.stop_pending
        and not st.session_state.manual_stop_status
    )
    LIVE_INDICATOR_COMPONENT(
        data={"active": is_active, "title": title},
        key=key,
    )

def render_log_console(
    lines: list[str],
    console_id: str,
) -> None:
    """Muestro y actualizo incrementalmente una consola de registros."""

    LOG_CONSOLE_COMPONENT(
        data={"lines": lines},
        height=250,
        key=console_id,
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
        "title": "Volumen negociado",
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

def stop_run_in_background(
    operation: dict[str, Any],
    controller_module: ModuleType,
    spark_module: ModuleType,
    run_id: str | None,
) -> None:
    """Detengo el RUN y libero sus recursos sin esperar la consolidación."""

    try:
        active_snapshot = controller_module.get_run_snapshot()
        stopping_run_id = run_id or active_snapshot.get("run_id")
        if stopping_run_id:
            functions.append_console_log(
                stopping_run_id,
                "controller",
                "Python | detención manual solicitada | finalizando proceso",
            )
        os.kill(os.getpid(), signal.SIGTERM)
        active_snapshot = controller_module.get_run_snapshot()
        run_id = run_id or active_snapshot.get("run_id")
        operation["debug"].append("solicitando detención del RUN")
        controller_module.request_stop_active_run("manual_stop")
        operation["debug"].append("RUN marcado para detener")
        operation["debug"].append("solicitando cierre de Spark")
        operation["spark_result"] = spark_module.stop_streaming(
            run_id,
            finalize=False,
        )
        operation["debug"].append("Spark finalizado")
        controller_module.stop_active_run("manual_stop")
        operation["debug"].append("Controller y Kafka finalizados")
    except Exception as error:
        operation["error"] = error
        operation["debug"].append(f"ERROR: {error}")
    finally:
        operation["debug"].append("STOP terminado")
        operation["done"] = True

def _get_consoles_context() -> tuple[Any, ...]:
    """Obtengo el contexto compartido para las seis consolas."""

    controller_module = load_page_module("01_python_controller")
    streaming_module = load_page_module("02_alpaca_stream")
    kafka_module = load_page_module("03_apache_kafka")
    spark_module = load_page_module("04_apache_spark")
    load_page_module("05_pandas")
    load_page_module("06_postgresql")
    controller_module.configure_streaming_module(streaming_module)
    controller_module.configure_pipeline_modules(
        kafka_module,
        spark_module,
    )
    snapshot = controller_module.get_run_snapshot()
    clear_consoles = bool(
        st.session_state.start_requested
        or st.session_state.start_pending
    )
    run_id = None if st.session_state.start_pending else snapshot["run_id"]
    paths = (
        DATA_DIRECTORY / "runs" / f"{run_id}_01_python_controller_log.txt",
        DATA_DIRECTORY / "runs" / f"{run_id}_02_alpaca_stream_log.txt",
        DATA_DIRECTORY / "runs" / f"{run_id}_03_apache_kafka_log.txt",
        DATA_DIRECTORY / "runs" / f"{run_id}_04_apache_spark_log.txt",
        DATA_DIRECTORY / "runs" / f"{run_id}_05_pandas_log.txt",
        DATA_DIRECTORY / "runs" / f"{run_id}_06_postgresql_log.txt",
    ) if run_id else (None,) * 6
    is_active = bool(
        snapshot["is_active"]
        and not st.session_state.start_pending
        and not st.session_state.stop_pending
    )
    return clear_consoles, run_id, *paths, is_active

def _has_active_spark_query(module: ModuleType) -> bool:
    """Consulto Spark sin propagar errores de conexión a la interfaz."""

    try:
        return bool(
            getattr(module, "has_active_query", lambda: False)()
        )
    except Exception:
        return False

def _refresh_heartbeat_display(
    component_key: str,
    register: bool = False,
) -> None:
    """Muestro el último heartbeat y lo renuevo solo desde el ciclo central."""

    heartbeat_text = "-"
    heartbeat_debug = "-"
    active = False
    if st.session_state.get("stop_pending"):
        HEARTBEAT_REFRESH_COMPONENT(
            data={"active": False, "text": heartbeat_text, "debug": heartbeat_debug},
            key=component_key,
        )
        return
    controller_module = load_page_module("01_python_controller")
    service = controller_module.get_active_service()
    if service is not None and service.is_active():
        active = True
        run_id = service.get_snapshot().get("run_id")
    else:
        run_id = None
    if run_id and (
        not register or register_heartbeat_for_display(controller_module, run_id)
    ):
        service_snapshot = service.get_snapshot()
        heartbeat_text = format_presentation_timestamp(
            service_snapshot.get("last_heartbeat"),
        ).split()[-1]
        heartbeat_debug = (
            f"Heartbeat ID: {service_snapshot.get('heartbeat_id', '-')} | "
            f"Sesión actual: {controller_module.get_heartbeat_session_id()} | "
            f"Última sesión: {service_snapshot.get('last_heartbeat_session_id') or '-'} | "
            f"Heartbeats: {service_snapshot.get('heartbeat_count', 0)}"
        )
    HEARTBEAT_REFRESH_COMPONENT(
        data={"active": active, "text": heartbeat_text, "debug": heartbeat_debug},
        key=component_key,
    )

### Intervalos de refresco.

render_heartbeat = st.fragment(render_heartbeat, run_every="5s")
render_config = st.fragment(render_config, run_every="5s")
render_controller_console = st.fragment(render_controller_console, run_every="200ms") # Especial: 200ms.
render_alpaca_console = st.fragment(render_alpaca_console, run_every="5s")
render_kafka_console = st.fragment(render_kafka_console, run_every="5s")
render_spark_console = st.fragment(render_spark_console, run_every="5s")
render_pandas_console = st.fragment(render_pandas_console, run_every="5s")
render_postgresql_console = st.fragment(render_postgresql_console, run_every="5s")
render_info = st.fragment(render_info, run_every="5s")
render_questions = st.fragment(render_questions, run_every="5s")
render_charts = st.fragment(render_charts, run_every="5s")
