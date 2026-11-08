import os
import re
import threading

from pathlib import Path

_CONSOLE_LOG_LOCK = threading.RLock()
_CONSOLE_LOG_SEQUENCES: dict[tuple[str, str], int] = {}
DATA_DIRECTORY = Path(__file__).parent / "data"

def append_console_log(
    run_id: str,
    console_name: str,
    line: str,
) -> None:
    """Persisto y publico una línea numerada de cualquier consola."""

    if console_name == "alpaca":
        file_name = "02_alpaca_stream_log.txt"
    elif console_name == "controller":
        file_name = "01_python_controller_log.txt"
    elif console_name == "kafka":
        file_name = "03_apache_kafka_log.txt"
    elif console_name == "spark":
        file_name = "04_apache_spark_log.txt"
    elif console_name == "pandas_postgresql":
        file_name = "05_pandas_postgresql_log.txt"
    else:
        raise ValueError(f"Consola no válida: {console_name}")
    key = (run_id, console_name)
    path = DATA_DIRECTORY / "runs" / f"{run_id}_{file_name}"
    with _CONSOLE_LOG_LOCK:
        sequence = _CONSOLE_LOG_SEQUENCES.get(key)
        if sequence is None:
            sequence = read_console_tail(path, 1)[2]
        sequence += 1
        _CONSOLE_LOG_SEQUENCES[key] = sequence
        persisted_line = f"[#{sequence}] {line}"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as log_file:
            log_file.write(persisted_line + "\n")
            log_file.flush()
            os.fsync(log_file.fileno())

def console_line_number(line: str) -> int:
    """Obtengo la numeración persistida de una línea de consola."""

    match = re.match(r"\[#(\d+)\]", line)
    return int(match.group(1)) if match else 0

def read_console_tail(
    path: Path,
    line_limit: int = 1000,
) -> tuple[list[str], int, int]:
    """Leo las últimas líneas de un TXT y devuelvo su cursor final."""

    if not path.exists() or line_limit <= 0:
        return [], 0, 0
    records: list[str] = []
    try:
        with path.open("rb") as console_file:
            file_size = console_file.seek(0, os.SEEK_END)
            position = file_size
            pending = b""
            while position > 0 and len(records) < line_limit:
                block_size = min(8192, position)
                position -= block_size
                console_file.seek(position)
                pending = console_file.read(block_size) + pending
                raw_lines = pending.split(b"\n")
                pending = raw_lines.pop(0)
                for raw_line in reversed(raw_lines):
                    if raw_line:
                        records.append(raw_line.decode("utf-8").rstrip("\r"))
                    if len(records) == line_limit:
                        break
            if pending and len(records) < line_limit:
                records.append(pending.decode("utf-8").rstrip("\r"))
    except (OSError, UnicodeDecodeError):
        return [], 0, 0
    records.reverse()
    return records, file_size, console_line_number(records[-1]) if records else 0
