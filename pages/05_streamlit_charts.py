"""Visualización Streamlit de resultados analíticos producidos por Spark."""

import streamlit as st

from datetime import datetime, timedelta, timezone
from typing import Any

def get_activity_chart_rows(
    spark_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Transformo la serie agregada por Spark a la escala visual."""

    if not spark_rows:
        return [], 1
    rows_by_second: dict[str, dict[str, int]] = {}
    for row in spark_rows:
        timestamp = datetime.fromisoformat(
            str(row["second"]).replace("Z", "+00:00")
        )
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        second = timestamp.astimezone(PRESENTATION_TIMEZONE).isoformat()
        second_counts = rows_by_second.setdefault(second, {})
        event_type = str(row["event_type"])
        second_counts[event_type] = second_counts.get(event_type, 0) + int(
            row["events"]
        )
    first_second = datetime.fromisoformat(min(rows_by_second))
    last_second = datetime.fromisoformat(max(rows_by_second))
    elapsed_seconds = (last_second - first_second).total_seconds()
    if elapsed_seconds <= 30:
        interval_seconds = 1
    elif elapsed_seconds <= 120:
        interval_seconds = 5
    elif elapsed_seconds <= 900:
        interval_seconds = 30
    elif elapsed_seconds <= 3600:
        interval_seconds = 60
    else:
        interval_seconds = 300
    grouped_counts: dict[str, dict[str, int]] = {}
    for second, event_counts in rows_by_second.items():
        timestamp = datetime.fromisoformat(second)
        seconds_from_midnight = (
            timestamp.hour * 3600
            + timestamp.minute * 60
            + timestamp.second
        )
        bucket_timestamp = timestamp - timedelta(
            seconds=seconds_from_midnight % interval_seconds,
        )
        bucket = bucket_timestamp.isoformat()
        bucket_counts = grouped_counts.setdefault(bucket, {})
        for event_type, count in event_counts.items():
            bucket_counts[event_type] = (
                bucket_counts.get(event_type, 0) + count
            )
    return [
        {
            "event_type": event_type,
            "events": count,
            "second": second,
        }
        for second in sorted(grouped_counts)
        for event_type, count in sorted(grouped_counts[second].items())
    ], interval_seconds
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
