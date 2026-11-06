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
