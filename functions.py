"""Funciones comunes para las consolas persistentes del laboratorio."""

import json
import os
import re
import threading

from collections import deque
from pathlib import Path

_CONSOLE_BUFFER_LOCK = threading.RLock()
_CONSOLE_BUFFERS: dict[tuple[str, str], deque[str]] = {}
_CONSOLE_BUFFER_SEEDED: set[tuple[str, str]] = set()
_CONSOLE_BUFFER_TOTALS: dict[tuple[str, str], int] = {}
CONSOLE_BUFFER_SIZE = 200

def append_console_buffer(
    run_id: str,
    console_name: str,
    line: str,
) -> None:
    """Agrego un registro al buffer visual acotado del RUN."""

    sequence = console_line_number(line)
    key = (run_id, console_name)
    with _CONSOLE_BUFFER_LOCK:
        buffer = _CONSOLE_BUFFERS.setdefault(
            key,
            deque(maxlen=CONSOLE_BUFFER_SIZE),
        )
        if sequence and any(
            console_line_number(existing) == sequence
            for existing in buffer
        ):
            return
        buffer.append(line)
        if sequence:
            _CONSOLE_BUFFER_TOTALS[key] = max(
                sequence,
                _CONSOLE_BUFFER_TOTALS.get(key, 0),
            )
        _CONSOLE_BUFFER_SEEDED.add(key)

def console_line_number(line: str) -> int:
    """Obtengo la numeración persistida de una línea de consola."""

    match = re.match(r"\[#(\d+)\]", line)
    return int(match.group(1)) if match else 0

def discard_console_buffers_except(run_id: str) -> None:
    """Libero buffers visuales de RUN anteriores para acotar la memoria."""

    with _CONSOLE_BUFFER_LOCK:
        previous_keys = [
            key
            for key in _CONSOLE_BUFFERS
            if key[0] != run_id
        ]
        for key in previous_keys:
            _CONSOLE_BUFFERS.pop(key, None)
            _CONSOLE_BUFFER_SEEDED.discard(key)
            _CONSOLE_BUFFER_TOTALS.pop(key, None)

def read_console_buffer(
    run_id: str,
    console_name: str,
    path: Path,
    cursor: int,
    initial_load: bool,
) -> tuple[list[str], int, int, bool]:
    """Devuelvo registros nuevos desde el buffer sin releer el TXT."""

    key = (run_id, console_name)
    with _CONSOLE_BUFFER_LOCK:
        seeded = key in _CONSOLE_BUFFER_SEEDED
    if not seeded or initial_load:
        tail_lines, _, _ = read_console_tail(path, CONSOLE_BUFFER_SIZE)
        with _CONSOLE_BUFFER_LOCK:
            buffer = _CONSOLE_BUFFERS.setdefault(
                key,
                deque(maxlen=CONSOLE_BUFFER_SIZE),
            )
            merged = {
                console_line_number(line): line
                for line in buffer
                if console_line_number(line)
            }
            merged.update(
                {
                    console_line_number(line): line
                    for line in tail_lines
                    if console_line_number(line)
                }
            )
            buffer.clear()
            buffer.extend(
                line
                for _, line in sorted(merged.items())[-CONSOLE_BUFFER_SIZE:]
            )
            _CONSOLE_BUFFER_TOTALS[key] = max(
                _CONSOLE_BUFFER_TOTALS.get(key, 0),
                *(merged or {0}),
            )
            _CONSOLE_BUFFER_SEEDED.add(key)
    with _CONSOLE_BUFFER_LOCK:
        buffer = _CONSOLE_BUFFERS.get(key, deque())
        lines = list(buffer)
        total = _CONSOLE_BUFFER_TOTALS.get(key, 0)
    if not lines:
        return [], cursor, total, False
    numbered_lines = [
        (console_line_number(line), line)
        for line in lines
        if console_line_number(line)
    ]
    numbered_lines.sort(key=lambda item: item[0])
    last_sequence = numbered_lines[-1][0]
    first_sequence = numbered_lines[0][0]
    resync = bool(cursor and cursor < first_sequence - 1)
    selected = (
        numbered_lines
        if initial_load or resync
        else [item for item in numbered_lines if item[0] > cursor]
    )
    return [line for _, line in selected], last_sequence, total, resync

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
