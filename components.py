import streamlit as st

EXECUTION_TIMER_COMPONENT = st.components.v2.component(
    name="apache_big_data_lab.execution_timer",
    html='<span class="execution-timer"></span>',
    isolate_styles=False,
    js="""
    export default function ({ parentElement, data }) {
        const timerElement = parentElement.querySelector(".execution-timer");
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
        };
        if (state.timerKey !== data.timer_key) {
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
