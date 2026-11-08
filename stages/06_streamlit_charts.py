import functions
import psycopg

from psycopg.rows import dict_row
from typing import Any

def get_market_analysis(run_id: str | None) -> dict[str, Any]:
    """Consulto PostgreSQL y devuelvo resultados ya preparados."""

    if not run_id:
        return {
            "activity_interval": 1,
            "activity_rows": [],
            "answers": [],
            "metrics": [],
            "top_volume": [],
        }
    try:
        with functions.get_postgresql_connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    "SELECT symbol, trade_count, return_count, volatility, spread_count, spread_mean, total_volume FROM market_metrics WHERE run_id = %s",
                    (run_id,),
                )
                metrics = cursor.fetchall()
                cursor.execute(
                    "SELECT question_id, feed, correlation, symbol_count, top_volume, activity_rows, activity_interval FROM market_answers WHERE run_id = %s",
                    (run_id,),
                )
                answers = cursor.fetchall()
                cursor.execute(
                    "SELECT top_volume, activity_rows, activity_interval FROM market_answers WHERE run_id = %s AND question_id = %s",
                    (run_id, "volatility"),
                )
                chart_answer = cursor.fetchone() or {}
        return {
            "activity_interval": chart_answer.get("activity_interval", 1),
            "activity_rows": chart_answer.get("activity_rows") or [],
            "answers": answers,
            "metrics": metrics,
            "top_volume": chart_answer.get("top_volume") or [],
        }
    except (OSError, psycopg.Error):
        return {
            "activity_interval": 1,
            "activity_rows": [],
            "answers": [],
            "metrics": [],
            "top_volume": [],
        }
