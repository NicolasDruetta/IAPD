import components
import renders
import streamlit as st

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

renders.render_header()
st.divider()
renders.render_config()
st.divider()
renders.render_consoles()
st.divider()
renders.render_info()
st.divider()
renders.render_charts()
st.divider()
renders.render_questions()
