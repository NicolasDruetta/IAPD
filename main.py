"""Interfaz principal de Apache Big Data Lab."""

import html
import importlib.util
import streamlit as st
import sys

from pathlib import Path
from types import ModuleType

def load_page_module(module_name: str) -> ModuleType:
    """Cargo un módulo de procesamiento sin construir su interfaz."""

    if module_name in sys.modules:
        return sys.modules[module_name]
    module_path = Path(__file__).parent / "pages" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        module_name, module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError("No se pudo cargar el módulo de Alpaca")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module

def render_log_console(lines: list[str], console_id: str) -> None:
    """Muestro el log al final y desplazo el textarea hasta el último registro.

    Mantengo el orden cronológico y enfoco automáticamente el último mensaje.
    """

    content = html.escape("\n".join(lines))
    st.html(
        f"""
        <textarea id="{console_id}" class="log-console" readonly aria-label="Consola">{content}</textarea>
        <script>
            (() => {{
                const consoleElement = document.getElementById("{console_id}");
                const scrollToBottom = () => {{
                    if (consoleElement) {{
                        consoleElement.scrollTop = consoleElement.scrollHeight;
                    }}
                }};
                scrollToBottom();
                requestAnimationFrame(scrollToBottom);
                setTimeout(scrollToBottom, 200);
            }})();
        </script>
        """,
        unsafe_allow_javascript=True,
    )

def load_custom_css() -> None:
    """Cargo los estilos visuales administrados en el archivo del proyecto."""

    css_path = Path(__file__).parent / "assets" / "custom.css"
    css_content = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

def initialize_state() -> None:
    """Creo el estado para conservar referencias visuales del RUN."""

    if "alpaca_run_id" not in st.session_state:
        st.session_state.alpaca_run_id = None

def render_streaming() -> None:
    """Muestro las consolas de Alpaca y el estado de Spark."""

    streaming_module = load_page_module("01_alpaca_streaming")
    spark_module = load_page_module("04_apache_spark")
    snapshot = streaming_module.get_run_snapshot()
    active_run_id = snapshot["run_id"] if snapshot["is_active"] else None
    session_run_id = st.session_state.alpaca_run_id
    if session_run_id is None and active_run_id is not None:
        st.session_state.alpaca_run_id = active_run_id
        session_run_id = active_run_id
    if session_run_id == active_run_id and active_run_id is not None:
        streaming_module.register_heartbeat(active_run_id)
        snapshot = streaming_module.get_run_snapshot()

    default_symbols = ", ".join(snapshot["symbols"])
    symbols_col, start_col, stop_col = st.columns(
        [6, 1.4, 1.4],
        vertical_alignment="bottom",
    )
    with symbols_col:
        symbols_text = st.text_input(
            "Símbolos",
            value=default_symbols or "AAPL, MSFT, NVDA, SPY",
        )
    symbols = sorted(
        {
            symbol.strip().upper()
            for symbol in symbols_text.split(",")
            if symbol.strip()
        }
    )
    with start_col:
        start_clicked = st.button(
            "Ejecutando streaming..."
            if snapshot["is_active"]
            else "Iniciar streaming",
            type="primary",
            disabled=bool(snapshot["is_active"]),
        )
    with stop_col:
        stop_clicked = st.button(
            "Detener streaming",
            disabled=not bool(snapshot["is_active"]),
        )
    selected_replay_speed = 1.0

    if start_clicked:
        if (
            not streaming_module.ALPACA_API_KEY
            or not streaming_module.ALPACA_SECRET_KEY
        ):
            st.error(
                "Completá ALPACA_API_KEY y ALPACA_SECRET_KEY en "
                "pages/01_alpaca_streaming.py."
            )
        elif not symbols:
            st.error("Indicá al menos un símbolo.")
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

    if stop_clicked:
        streaming_module.stop_active_run("manual_stop")
        snapshot = streaming_module.get_run_snapshot()

    status = snapshot["status"]
    status_col, count_col, mode_col, run_col = st.columns(4)
    with status_col:
        st.metric("Estado", status)
    with count_col:
        st.metric("Eventos recibidos", snapshot["event_count"])
    with mode_col:
        st.metric("Modo", snapshot["mode"])
    with run_col:
        run_id = snapshot["run_id"] or "-"
        st.metric("RUN", run_id[:12])

    if snapshot["error"]:
        st.error(snapshot["error"])

    events = streaming_module.get_recent_events(snapshot["run_id"], 100)
    spark_result = spark_module.process_events(events)
    execution_column, alpaca_column, spark_column = st.columns(
        3,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with execution_column:
        st.header("Log de ejecución")
        st.caption("Ciclo de vida, conexión y procesamiento del RUN actual")
        log_lines = streaming_module.get_recent_logs(
            snapshot["run_id"],
            100,
        )
        render_log_console(log_lines, "execution-log-console")
    with alpaca_column:
        st.header(f"Alpaca Streaming")
        st.caption(
            "LIVE: operaciones actuales; REPLAY: operaciones históricas."
        )
        event_lines = [
            event.to_display_line()
            for event in events
        ]
        render_log_console(event_lines, "alpaca-log-console")
        if not event_lines and status == "conectado":
            st.info(
                "Conexión activa; todavía no se recibieron operaciones. "
                "El mercado puede estar cerrado."
            )
    with spark_column:
        st.header("Apache Spark")
        st.caption("Procesamiento local de un lote acotado por símbolo")
        spark_lines = list(spark_result["logs"])
        if spark_result["rows"]:
            spark_lines.append("")
            spark_lines.append("Resultados por símbolo:")
            spark_lines.extend(
                "{symbol} | operaciones={operations} | volumen={volume}".format(
                    symbol=row["symbol"],
                    operations=row["operations"],
                    volume=row["total_volume"] or 0,
                )
                for row in spark_result["rows"]
            )
        render_log_console(spark_lines, "spark-log-console")

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="📊",
    layout="wide",
)

load_custom_css()
initialize_state()

st.title("Apache Big Data Lab")
st.subheader("Procesamiento distribuido y análisis de mercados financieros")
render_streaming_fragment = st.fragment(render_streaming, run_every="5s")
render_streaming_fragment()
