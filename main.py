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
st.subheader("Configuración")
renders.render_config()
st.divider()
st.subheader("Consolas parte 1: Descarga masiva y streaming de datos en batches")
renders.render_consoles_part1()
st.divider()
st.subheader("Consolas parte 2: Lectura desde archivos y persistenca en base de datos")
renders.render_consoles_part2()
st.divider()
st.subheader("Información")
renders.render_info()
st.divider()
st.subheader("Gráficos")
renders.render_charts()
st.divider()
st.subheader("Preguntas de investigación")
renders.render_questions()
st.divider()
renders.render_footer()
