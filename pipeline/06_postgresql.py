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
        postgresql_log_path = (
            functions.DATA_DIRECTORY
            / "runs"
            / f"{run_id}_06_postgresql_log.txt"
        )
        postgresql_log_lines, _, _ = functions.read_console_tail(
            postgresql_log_path,
            1000,
        )
        if not any(
            "POSTGRES | inicialización confirmada" in line
            for line in postgresql_log_lines
        ):
            functions.append_console_log(
                run_id,
                "postgresql",
                f"POSTGRES | inicialización confirmada | run_id={run_id} | conexión y tablas disponibles",
            )
        if not any(
            "POSTGRES | persistencia confirmada" in line
            for line in postgresql_log_lines
        ):
            functions.append_console_log(
                run_id,
                "postgresql",
                f"POSTGRES | persistencia confirmada | run_id={run_id} | resultados_enviados={len(analysis['metrics'])} | respuestas_enviadas={len(analysis['answers'])}",
            )
        return True
    except (OSError, TypeError, ValueError, psycopg.Error) as error:
        functions.append_console_log(
            run_id,
            "postgresql",
            f"ERROR | persistencia={error}",
        )
        return False
