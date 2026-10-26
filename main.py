"""Interfaz principal de Apache Big Data Lab."""

import importlib.util
import streamlit as st

from collections import deque
from pathlib import Path
from types import ModuleType

def load_page_module(module_name: str) -> ModuleType:
    """Cargo un módulo de procesamiento sin construir su interfaz."""

    module_path = Path(__file__).parent / "pages" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        module_name, module_path
    )
    if spec is None or spec.loader is None:
        raise ImportError("No se pudo cargar el módulo de Alpaca")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def get_page_module(module_name: str) -> ModuleType:
    """Conservo una sola carga de cada módulo durante la sesión."""

    module_key = f"page_module_{module_name}"
    if module_key not in st.session_state:
        st.session_state[module_key] = load_page_module(module_name)
    return st.session_state[module_key]

def load_custom_css() -> None:
    """Cargo los estilos visuales centralizados del proyecto."""

    css_path = Path(__file__).parent / "assets" / "custom.css"
    css_content = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

def initialize_state() -> None:
    """Creo el estado para conservar la conexión entre actualizaciones."""

    if "alpaca_service" not in st.session_state:
        st.session_state.alpaca_service = None
    if "alpaca_events" not in st.session_state:
        st.session_state.alpaca_events = deque(maxlen=100)

def render_streaming() -> None:
    """Muestro controles y eventos reales recibidos desde Alpaca."""

    streaming_module = get_page_module("01_alpaca_streaming")
    service = st.session_state.alpaca_service

    st.header("Alpaca Streaming — Mercado en tiempo real")
    st.caption(
        "Los eventos se reciben desde Alpaca Market Data y no se generan "
        "localmente."
    )

    symbols_text = st.text_input("Símbolos", value="AAPL, MSFT, NVDA, SPY")
    symbols = sorted(
        {
            symbol.strip().upper()
            for symbol in symbols_text.split(",")
            if symbol.strip()
        }
    )
    start_col, stop_col, status_col, count_col = st.columns(4)
    with start_col:
        start_clicked = st.button(
            "Iniciar streaming",
            type="primary",
            disabled=bool(service and service.is_running()),
        )
    with stop_col:
        stop_clicked = st.button(
            "Detener streaming",
            disabled=not bool(service and service.is_running()),
        )

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
            if service is None:
                service = streaming_module.create_service(
                    streaming_module.ALPACA_API_KEY,
                    streaming_module.ALPACA_SECRET_KEY,
                )
                st.session_state.alpaca_service = service
            service.start(symbols)

    if stop_clicked and service is not None:
        service.stop()

    service = st.session_state.alpaca_service
    status = service.get_status() if service is not None else "detenido"
    with status_col:
        st.metric("Estado", status)
    with count_col:
        st.metric("Eventos recibidos", len(st.session_state.alpaca_events))

    if service is not None:
        st.session_state.alpaca_events.extend(service.drain_events())
        if service.get_error():
            st.error(service.get_error())

    event_lines = [
        event.to_display_line() for event in st.session_state.alpaca_events
    ]
    st.text_area(
        "Consola de eventos",
        value="\n".join(reversed(event_lines)),
        height=320,
        disabled=True,
    )
    if not event_lines and status == "conectado":
        st.info(
            "Conexión activa; todavía no se recibieron operaciones. "
            "El mercado puede estar cerrado."
        )

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="📊",
    layout="wide",
)

load_custom_css()
initialize_state()

st.title("Apache Big Data Lab")
st.subheader("Procesamiento distribuido y análisis de mercados financieros")
render_streaming_fragment = st.fragment(render_streaming, run_every="1s")
render_streaming_fragment()
