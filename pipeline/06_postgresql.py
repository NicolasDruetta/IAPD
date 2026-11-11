# Autor: Nicolás Druetta.

import psycopg

from app import database, functions
from typing import Any

FEED_NAME = "IEX"
_STATUS_CALLBACK: object | None = None

def configure_status_callback(callback: object | None) -> None:
    """Registro el callback de confirmación de la primera persistencia."""

    global _STATUS_CALLBACK
    _STATUS_CALLBACK = callback

def persist_analysis(
    run_id: str,
    analysis: dict[str, Any],
    feed: str = FEED_NAME,
) -> bool:
    """Persisto resultados analíticos recibidos del Controller."""

    try:
        if _STATUS_CALLBACK is not None:
            _STATUS_CALLBACK(run_id, "PostgreSQL_start")
        with database.get_postgresql_connection() as connection:
            database.ensure_tables(connection)
            persistence_counts = database.persist_analysis(
                connection,
                run_id,
                analysis["metrics"],
                analysis["answers"],
                analysis["top_volume"],
                analysis["activity"],
                analysis["activity_interval"],
            )
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT
                            (SELECT COUNT(*) FROM {database.MARKET_METRICS_TABLE} WHERE run_id = %s),
                            (SELECT COUNT(*) FROM {database.MARKET_ANSWERS_TABLE} WHERE run_id = %s)
                        """,
                        (run_id, run_id),
                    )
                    metrics_count, answers_count = cursor.fetchone()
            except (OSError, TypeError, ValueError, psycopg.Error) as error:
                functions.append_console_log(
                    run_id,
                    "postgresql",
                    f"ERROR | conteo_postpersistencia={error} | persistencia_confirmada=True",
                )
                return True
        if _STATUS_CALLBACK is not None:
            _STATUS_CALLBACK(run_id, "PostgreSQL_success")
        previous_counts = functions.get_postgresql_counters(
            functions.DATA_DIRECTORY / "runs" / f"{run_id}_06_postgresql_log.txt"
        )
        total_count = int(metrics_count) + int(answers_count)
        events_count = previous_counts["events"] + 1
        updates_count = previous_counts["updates"] + persistence_counts["updated"]
        functions.append_console_log(
            run_id,
            "postgresql",
            f"POSTGRES | run_id={run_id} | eventos_recibidos={events_count} | métricas={int(metrics_count)} | respuestas={int(answers_count)} | filas_totales={total_count} | actualizaciones={updates_count}",
        )
        return True
    except (OSError, TypeError, ValueError, psycopg.Error) as error:
        if _STATUS_CALLBACK is not None:
            _STATUS_CALLBACK(run_id, "PostgreSQL_error", str(error))
        functions.append_console_log(
            run_id,
            "postgresql",
            f"ERROR | persistencia={error}",
        )
        raise
