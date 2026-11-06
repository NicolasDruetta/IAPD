import components
import renders
import streamlit as st

from pathlib import Path

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="assets/icon.png",
    layout="wide",
)

renders.load_custom_css()

system_theme = components.SYSTEM_THEME_COMPONENT(
    data={},
    default={"dark": None},
    key="system-theme-detector",
    on_dark_change=lambda: None,
)

renders.initialize_state(system_theme)

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

st.subheader("Streaming y análisis de operaciones bursátiles con procesamiento distribuido durante una rueda de mercado.")
st.caption("Materia: IAPD. Autor: Nicolás Druetta. Año: 2026.")
st.divider()

render_streaming_fragment = st.fragment(renders.render_streaming, run_every="5s")
render_streaming_fragment()
