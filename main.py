"""Interfaz principal de Apache Big Data Lab."""

import html
import importlib.util
import json
import streamlit as st
import sys

from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

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
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        module._SOURCE_MTIME_NS = source_mtime_ns
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

def read_all_streaming_events(
    streaming_module: ModuleType,
    run_id: str | None,
) -> list[Any]:
    """Leo todos los eventos persistidos sin aplicar un límite."""

    if run_id is None:
        return []
    events_path = streaming_module.DATA_DIRECTORY / f"{run_id}.jsonl"
    if not events_path.exists():
        return []
    events = []
    try:
        with events_path.open(encoding="utf-8") as events_file:
            for line in events_file:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
                    events.append(streaming_module.event_from_record(record))
    except OSError:
        return []
    return events

def load_custom_css() -> None:
    """Cargo los estilos visuales administrados en el archivo del proyecto."""

    css_path = Path(__file__).parent / "assets" / "custom.css"
    css_content = css_path.read_text(encoding="utf-8")
    render_html(f"<style>{css_content}</style>")

def render_html(content: str) -> None:
    """Renderizo HTML habilitado desde un único punto."""

    st.markdown(content, unsafe_allow_html=True)

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

def initialize_state() -> None:
    """Creo el estado para conservar referencias visuales del RUN."""

    if "alpaca_run_id" not in st.session_state:
        st.session_state.alpaca_run_id = None
    if "start_pending" not in st.session_state:
        st.session_state.start_pending = False
    if "stop_pending" not in st.session_state:
        st.session_state.stop_pending = False

def mark_start_pending() -> None:
    """Marco que el usuario acaba de solicitar el inicio."""

    st.session_state.start_pending = True

def mark_stop_pending() -> None:
    """Marco que el usuario acaba de solicitar la detención."""

    st.session_state.stop_pending = True

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
    if not snapshot["is_active"]:
        default_symbols = (
            "AAPL, MSFT, NVDA, SPY, TSLA, META, SPCX, GLD, BTC, OXY"
        )
    render_html("<h5>Símbolos</h5>")
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
    selected_replay_speed = 1.0

    if start_clicked or st.session_state.start_pending:
        st.session_state.start_pending = False
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
            streaming_module.format_display_timestamp(last_execution_at)
            if last_execution_at
            else "-"
        )
        status_text = (
            f"{status_text}. Transcurrido: {elapsed_text}. "
            f"Ejecución: {last_execution_text}."
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
            render_html(
                f'<div class="metric-card"><p><strong>RUN:</strong> '
                f'{html.escape(run_id[:12])}</p></div>'
            )
        with mode_col:
            render_html(
                f'<div class="metric-card"><p><strong>Modo:</strong> '
                f'{html.escape(snapshot["mode"])}</p></div>'
            )
        with count_col:
            render_html(
                f'<div class="metric-card"><p>'
                f'<strong>Eventos recibidos:</strong> '
                f'{snapshot["event_count"]}</p></div>'
            )
    with status_col:
        render_html(
            f'<div class="metric-card"><p><strong>Estado:</strong> '
            f'{html.escape(status_text)}</p></div>'
        )

    if snapshot["error"]:
        st.error(snapshot["error"])

    render_html("<hr />")

    try:
        events = streaming_module.get_recent_events(snapshot["run_id"])
    except TypeError as error:
        if "required positional argument: 'limit'" not in str(error):
            raise
        events = read_all_streaming_events(
            streaming_module,
            snapshot["run_id"],
        )
    spark_result = spark_module.process_events(events)
    execution_column, alpaca_column, spark_column = st.columns(
        3,
        gap="small",
        vertical_alignment="top",
        wrap=True,
    )
    with execution_column:
        render_html("<h5>Log de ejecución</h5>")
        render_html(
            "<p>Ciclo de vida, conexión y procesamiento del RUN actual</p>",
        )
        log_lines = snapshot.get("logs", [])
        render_log_console(log_lines, "execution-log-console")
    with alpaca_column:
        render_html("<h5>Alpaca Streaming</h5>")
        render_html(
            "<p>LIVE: operaciones actuales; REPLAY: operaciones históricas.</p>",
        )
        event_lines = [
            event.to_display_line()
            for event in events
        ]
        render_log_console(event_lines, "alpaca-log-console")
        if not event_lines and status == "Conectado":
            st.info(
                "Conexión activa; todavía no se recibieron operaciones. "
                "El mercado puede estar cerrado."
            )
    with spark_column:
        render_html(
            '<h5>Apache Spark - <a class="spark-ui-link" href="http://localhost:8080/" '
            'target="_blank" rel="noopener noreferrer"><small>Spark Master</small> '
            '<span class="spark-ui-link-icon" aria-hidden="true">↗</span>'
            '</a></h5>'
        )
        render_html(
            "<p>Procesamiento local de un lote acotado por símbolo</p>",
        )
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
    page_icon="assets/icon.png",
    layout="wide",
)

load_custom_css()
initialize_state()

icon_path = Path(__file__).parent / "assets" / "icon.png"
icon_col, title_col = st.columns(
    [0.4, 11.6],
    gap="small",
    vertical_alignment="center",
)
with icon_col:
    with st.container(key="header-icon"):
        st.image(str(icon_path), width=40)
with title_col:
    render_html("<h1>Apache Big Data Lab</h1>")
render_html("<h4>Procesamiento distribuido y análisis de mercados financieros</h4>")
render_html("<label>Alumno: Nicolás Druetta. Materia: IAPD. Año: 2026.</label><hr />")
render_streaming_fragment = st.fragment(render_streaming, run_every="5s")
render_streaming_fragment()
