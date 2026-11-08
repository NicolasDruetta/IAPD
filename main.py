import streamlit as st

from app import renders

st.set_page_config(
    page_title="Apache Big Data Lab",
    page_icon="assets/icon.png",
    layout="wide",
)

renders.load_custom_css()

system_theme = renders.SYSTEM_THEME_COMPONENT(
    data={},
    default={"dark": None},
    key="system-theme-detector",
    on_dark_change=lambda: None,
)

renders.initialize_state(system_theme)

renders.render_header()
st.divider()
st.subheader("CONFIGURACIÓN")
renders.render_config()
st.divider()
st.subheader("CONSOLAS")
renders.render_consoles()
st.divider()
st.subheader("INFORMACIÓN")
renders.render_info()
st.divider()
st.subheader("GRÁFICOS")
renders.render_charts()
st.divider()
st.subheader("PREGUNTAS DE INVESTIGACIÓN")
renders.render_questions()
