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
            database.persist_analysis(
                connection,
                run_id,
                analysis["metrics"],
                analysis["answers"],
                analysis["top_volume"],
                analysis["activity"],
                analysis["activity_interval"],
            )
        functions.append_console_log(
            run_id,
            "postgresql",
            f"POSTGRES | persistencia confirmada | run_id={run_id} | métricas={len(analysis['metrics'])} | respuestas={len(analysis['answers'])}",
        )
        return True
    except (OSError, TypeError, ValueError, psycopg.Error) as error:
        functions.append_console_log(
            run_id,
            "postgresql",
            f"ERROR | persistencia={error}",
        )
        return False
