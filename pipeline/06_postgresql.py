import psycopg
import sys

from app import database, functions

FEED_NAME = "IEX"

def analyze_and_persist(
    run_id: str,
    feed: str = FEED_NAME,
) -> bool:
    """Persisto en PostgreSQL los resultados preparados por Pandas."""

    analysis_module = sys.modules.get("05_pandas")
    if analysis_module is None:
        functions.append_console_log(
            run_id,
            "postgresql",
            "ERROR | persistencia=análisis Pandas no disponible",
        )
        return False
    try:
        analysis = analysis_module.analyze(run_id, feed)
        if analysis is None:
            functions.append_console_log(
                run_id,
                "postgresql",
                "ERROR | persistencia=análisis Pandas no disponible",
            )
            return False
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
        functions.append_console_log(
            run_id,
            "postgresql",
            f"ERROR | persistencia={error}",
        )
        return False
