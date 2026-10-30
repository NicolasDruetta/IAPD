"""Obtención de cotizaciones y operaciones bursátiles mediante Alpaca."""

import asyncio
import certifi
import heapq
import itertools
import json
import os
import queue
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable
from websockets.asyncio.client import ClientConnection, connect
from zoneinfo import ZoneInfo

ALPACA_API_KEY = "PKJQMFWVYH6PEXUKK34QAABOUW"
ALPACA_SECRET_KEY = "5xBskyt9ofjunrzuCa77RDwjP5n5fGCDDv6XrfxqzAyk"
ALL_MARKET_SYMBOL = "*"
ASSETS_API_URLS = (
    "https://paper-api.alpaca.markets/v2/assets",
    "https://api.alpaca.markets/v2/assets",
)
CALENDAR_API_URLS = (
    "https://api.alpaca.markets/v2/calendar",
    "https://paper-api.alpaca.markets/v2/calendar",
)
DATA_DIRECTORY = Path(__file__).resolve().parent.parent / "data"
HISTORICAL_QUOTES_API_URL = "https://data.alpaca.markets/v2/stocks/quotes"
HISTORICAL_TRADES_API_URL = "https://data.alpaca.markets/v2/stocks/trades"
HISTORICAL_DIRECTORY = DATA_DIRECTORY / "historical"
HTTP_MAX_RETRIES = 5
LEGACY_STREAMING_DIRECTORY = DATA_DIRECTORY / "streaming"
MARKET_CLOCK_URLS = (
    "https://api.alpaca.markets/v2/clock",
    "https://paper-api.alpaca.markets/v2/clock",
)
MAX_HTTP_CONCURRENCY = 4
NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")
RANKED_SYMBOL_COUNT = 20
REPLAY_BATCH_SIZE = 100
REPLAY_SPEEDS = {
    "1 evento por segundo": 1.0,
    "10 eventos por segundo": 0.1,
    "100 eventos por segundo": 0.01,
    "Máxima velocidad controlada": 0.0,
}
REPLAY_WINDOW_MINUTES = 15
SP500_COMPONENTS_URL = (
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
)
SP500_INDEX_URL = (
    "https://www.spglobal.com/spdji/en/indices/equity/sp-500/"
)
STOCK_SNAPSHOTS_URL = "https://data.alpaca.markets/v2/stocks/snapshots"
TEMPORARY_DIRECTORY = Path("/tmp")

class SP500TableParser(HTMLParser):
    """Extraigo filas de tablas HTML para localizar la columna Symbol."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._cell: list[str] | None = None
        self._row: list[str] | None = None
        self._table: list[list[str]] | None = None
        self._table_depth = 0
        self.tables: list[list[list[str]]] = []
        self.rows: list[list[str]] = []

    def handle_data(self, data: str) -> None:
        """Acumulo el texto de la celda actual."""

        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        """Cierro celdas y filas de la tabla analizada."""

        if tag in {"th", "td"} and self._cell is not None:
            cell_text = " ".join("".join(self._cell).split())
            if self._row is not None:
                self._row.append(cell_text)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                if self._table is not None:
                    self._table.append(self._row)
                self.rows.append(self._row)
            self._row = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0 and self._table is not None:
                self.tables.append(self._table)
                self._table = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        """Inicio el registro de una celda o fila."""

        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._table = []
        elif tag == "tr" and self._table_depth == 1:
            self._row = []
        elif (
            tag in {"th", "td"}
            and self._row is not None
            and self._table_depth == 1
        ):
            self._cell = []

class AlpacaEvent:
    """Represento un evento financiero recibido desde Alpaca."""

    timestamp: str
    symbol: str
    event_type: str
    price: float | None
    size: float | None
    bid_price: float | None
    bid_size: float | None
    ask_price: float | None
    ask_size: float | None
    raw: dict[str, Any]

    def __init__(
        self,
        timestamp: str,
        symbol: str,
        event_type: str,
        price: float | None,
        size: float | None,
        mode: str = "LIVE",
        console_sequence: int | None = None,
        bid_price: float | None = None,
        bid_size: float | None = None,
        ask_price: float | None = None,
        ask_size: float | None = None,
        raw: dict[str, Any] | None = None,
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.event_type = event_type
        self.price = price
        self.size = size
        self.bid_price = bid_price
        self.bid_size = bid_size
        self.ask_price = ask_price
        self.ask_size = ask_size
        self.raw = dict(raw or {})
        self.mode = mode
        self.console_sequence = console_sequence

    def to_display_line(self) -> str:
        """Convierto el evento a una línea segura para mostrarlo."""

        timestamp = format_presentation_timestamp(
            self.timestamp,
            source_timezone=timezone.utc,
        )
        if self.event_type == "QUOTE":
            return (
                f"{timestamp} | {self.symbol} | QUOTE | "
                f"bid={self._format_number(self.bid_price)} | "
                f"bid_size={self._format_number(self.bid_size)} | "
                f"ask={self._format_number(self.ask_price)} | "
                f"ask_size={self._format_number(self.ask_size)}"
            )
        price = self._format_number(self.price, decimals=4)
        size = self._format_number(self.size)
        return (
            f"{timestamp} | {self.symbol} | {self.event_type} | "
            f"{price} | {size}"
        )

    @staticmethod
    def _format_number(
        value: float | None,
        decimals: int | None = None,
    ) -> str:
        """Formateo un número sin perder la distinción de cada campo."""

        if value is None:
            return "-"
        if decimals is not None:
            return f"{value:.{decimals}f}"
        return f"{value:g}"

    def to_record(self, run_id: str) -> dict[str, Any]:
        """Convierto el evento al registro JSONL persistido."""

        return {
            "run_id": run_id,
            "received_at": get_utc_timestamp(),
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "event_type": self.event_type,
            "price": self.price,
            "size": self.size,
            "bid_price": self.bid_price,
            "bid_size": self.bid_size,
            "ask_price": self.ask_price,
            "ask_size": self.ask_size,
            "mode": self.mode,
            "raw": dict(self.raw),
        }

class AlpacaStreamingService:
    """Administro una ejecución Alpaca independiente de Streamlit."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        run_id: str,
        symbols: list[str],
        started_at: str,
        run_stem: str,
        universe_metadata: dict[str, Any],
        feed: str = "iex",
        mode: str = "LIVE",
        replay_speed: float = 1.0,
        release_callback: Callable[["AlpacaStreamingService"], None]
        | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._run_id = run_id
        self._run_stem = run_stem
        self._symbols = symbols
        self._universe_metadata = universe_metadata
        self._feed = feed
        self._mode = mode
        self._replay_speed = replay_speed
        self._release_callback = release_callback
        self._events: deque[AlpacaEvent] = deque()
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._stream_thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._websocket: ClientConnection | None = None
        self._status = "Listo para ejecutar"
        self._error: str | None = None
        self._end_reason: str | None = None
        self._started_at: str | None = None
        self._ended_at: str | None = None
        self._last_heartbeat_at: str | None = None
        self._last_heartbeat_monotonic = 0.0
        self._event_count = 0
        self._replay_batch_number = 0
        self._replay_position = 0
        self._historical_date: str | None = None
        self._finished = False
        self._log_count = 0
        self._spark_checkpoint: dict[str, Any] | None = None
        self._spark_console_initialized = False
        self._sequence = 0
        self._spark_checkpoint_processed = 0
        self._started_at = started_at
        self._console_sequences = {
            "alpaca": 0,
            "log": 0,
            "spark": 0,
        }
        self._historical_path = DATA_DIRECTORY / f"{run_stem}_data_alpaca.jsonl"
        self._run_info_path = DATA_DIRECTORY / f"{run_stem}_run_info.jsonl"
        self._log_handler_path = (
            DATA_DIRECTORY / f"{run_stem}_log_01_controller.txt"
        )
        self._log_alpaca_path = (
            DATA_DIRECTORY / f"{run_stem}_log_02_alpaca_stream.txt"
        )
        self._log_spark_path = (
            DATA_DIRECTORY / f"{run_stem}_log_03_apache_spark.txt"
        )
        self._streaming_path = self._run_info_path
        ensure_data_directory()
        if any(
            path.exists()
            for path in (
                self._run_info_path,
                self._historical_path,
                self._log_handler_path,
                self._log_alpaca_path,
                self._log_spark_path,
            )
        ):
            raise FileExistsError(f"El RUN ya existe: {run_stem}")
        self._run_info_path.touch(exist_ok=False)
        self._historical_path.touch(exist_ok=False)
        self._log_handler_path.touch(exist_ok=False)
        self._log_alpaca_path.touch(exist_ok=False)
        self._log_spark_path.touch(exist_ok=False)

    def start(self) -> bool:
        """Inicio los hilos de streaming y supervisión del RUN."""

        with self._lock:
            if self.is_active():
                return False
            self._status = (
                "Descargando histórico"
                if self._mode == "REPLAY"
                else "Conectando"
            )
            self._last_heartbeat_at = self._started_at
            self._last_heartbeat_monotonic = time.monotonic()
            self._append_run_info_locked(
                "run_started",
                {
                    "symbols": list(self._symbols),
                    "mode": self._mode,
                    "feed": self._feed,
                    "replay_speed": self._replay_speed,
                    "started_at": self._started_at,
                    "data_file": self._historical_path.name,
                    "run_info_file": self._run_info_path.name,
                    "log_controller_file": self._log_handler_path.name,
                    "log_alpaca_file": self._log_alpaca_path.name,
                    "log_spark_file": self._log_spark_path.name,
                    "universe": self._universe_metadata,
                    "console_schema_version": 3,
                },
            )
            self._add_log(f"{self._mode} | RUN iniciado")
            if self._universe_metadata.get("selection") == "sp500":
                self._add_log(
                    "{mode} | S&P 500 | componentes={components} | "
                    "disponibles={available} | no_disponibles={missing}"
                    .format(
                        mode=self._mode,
                        components=self._universe_metadata["component_count"],
                        available=self._universe_metadata["available_count"],
                        missing=self._universe_metadata["unavailable_count"],
                    )
                )
                self._add_log(
                    "{mode} | S&P 500 | fuente={source} | "
                    "actualizada={updated} | consultada={fetched}".format(
                        mode=self._mode,
                        source=self._universe_metadata["source"],
                        updated=self._universe_metadata["source_updated_at"],
                        fetched=self._universe_metadata["fetched_at"],
                    )
                )
                self._append_run_info_locked(
                    "universe",
                    self._universe_metadata,
                )
            self._append_state_locked()
            self._stream_thread = threading.Thread(
                target=self._run_stream,
                daemon=True,
                name=f"alpaca-stream-{self._run_id}",
            )
            self._stream_thread.start()
            self._add_log(f"{self._mode} | worker de supervisión iniciado")
            self._append_state_locked()
        return True

    def heartbeat(self, run_id: str) -> bool:
        """Registro una señal válida proveniente de la sesión visual."""

        with self._lock:
            if run_id != self._run_id or not self.is_active():
                return False
            self._last_heartbeat_monotonic = time.monotonic()
            self._last_heartbeat_at = get_utc_timestamp()
            self._append_state_locked()
        return True

    def heartbeat_elapsed(self) -> float:
        """Devuelvo los segundos desde el último heartbeat válido."""

        with self._lock:
            return time.monotonic() - self._last_heartbeat_monotonic

    def stop(self, end_reason: str) -> None:
        """Detengo el RUN y solicito el cierre real del WebSocket."""

        with self._lock:
            if self._finished:
                return
            self._status = "Finalizando"
            self._end_reason = end_reason
            self._stop_event.set()
            self._add_log(f"{self._mode} | detención solicitada: {end_reason}")
            loop = self._loop
            task = self._task
            websocket = self._websocket
            self._append_state_locked()
        if loop is not None and task is not None:
            loop.call_soon_threadsafe(task.cancel)
        if loop is not None and websocket is not None:
            close_future = asyncio.run_coroutine_threadsafe(
                websocket.close(),
                loop,
            )
            try:
                close_future.result(timeout=2)
            except Exception:
                pass
        self._join_threads()
        if not self.is_running():
            self._finish("Detenido", end_reason)

    def is_active(self) -> bool:
        """Indico si el RUN sigue aceptando heartbeats."""

        return not self._finished and self._status in {
            "Conectando",
            "Conectado",
            "Descargando histórico",
            "Reproduciendo",
            "Finalizando",
        }

    def is_running(self) -> bool:
        """Indico si el hilo del WebSocket todavía está activo."""

        return bool(self._stream_thread and self._stream_thread.is_alive())

    def get_snapshot(self) -> dict[str, Any]:
        """Devuelvo el estado actual del RUN sin exponer credenciales."""

        with self._lock:
            return {
                "run_id": self._run_id,
                "symbols": list(self._symbols),
                "universe": self._universe_metadata,
                "mode": self._mode,
                "replay_speed": self._replay_speed,
                "status": self._status,
                "error": self._error,
                "event_count": self._event_count,
                "log_count": self._log_count,
                "replay_position": self._replay_position,
                "historical_date": self._historical_date,
                "logs": [],
                "console_schema_version": 3,
                "started_at": self._started_at,
                "ended_at": self._ended_at,
                "last_heartbeat": self._last_heartbeat_at,
                "end_reason": self._end_reason,
                "run_info_file": self._run_info_path.name,
                "data_file": self._historical_path.name,
                "log_controller_file": self._log_handler_path.name,
                "log_alpaca_file": self._log_alpaca_path.name,
                "log_spark_file": self._log_spark_path.name,
                "is_active": self.is_active(),
            }

    def get_events_after(self, processed_count: int) -> list[AlpacaEvent]:
        """Devuelvo solo los eventos posteriores al checkpoint de Spark."""

        with self._lock:
            return list(itertools.islice(self._events, processed_count, None))

    def append_spark_checkpoint(
        self,
        checkpoint: dict[str, Any],
        console_lines: list[str],
    ) -> bool:
        """Persisto Spark y sus líneas visibles una sola vez."""

        processed_count = int(checkpoint.get("processed_events", 0))
        with self._lock:
            if processed_count <= self._spark_checkpoint_processed:
                return False
            for line in console_lines:
                if line == "Spark | esperando eventos bursátiles.":
                    continue
                self._append_console_line_locked("spark", line)
            self._append_run_info_locked("spark_checkpoint", checkpoint)
            self._spark_checkpoint_processed = processed_count
            self._spark_checkpoint = dict(checkpoint)
            self._spark_console_initialized = True
            return True

    def ensure_spark_console_records(self, console_lines: list[str]) -> None:
        """Persisto una única entrada inicial de la consola Spark."""

        with self._lock:
            if self._spark_console_initialized:
                return
            for line in console_lines:
                if line == "Spark | esperando eventos bursátiles.":
                    continue
                self._append_console_line_locked("spark", line)
            self._spark_console_initialized = True

    def get_spark_checkpoint(self) -> dict[str, Any] | None:
        """Devuelvo el checkpoint activo sin releer el JSONL."""

        with self._lock:
            return (
                dict(self._spark_checkpoint)
                if self._spark_checkpoint is not None
                else None
            )

    def _run_stream(self) -> None:
        """Ejecuto el ciclo asincrónico del WebSocket en un hilo propio."""

        try:
            self._add_log(f"{self._mode} | worker de datos iniciado")
            if self._mode == "REPLAY":
                self._run_replay()
            else:
                asyncio.run(self._receive_events())
        except asyncio.CancelledError:
            pass
        except Exception as error:
            if not self._stop_event.is_set():
                self._finish(
                    "Error",
                    "connection_error",
                    self._safe_error(error),
                )
        finally:
            self._clear_async_resources()
            if self._stop_event.is_set():
                self._finish("Detenido", self._end_reason or "manual_stop")

    def _run_replay(self) -> None:
        """Descargo y reproduzco eventos históricos de forma progresiva."""

        self._add_log("REPLAY | buscando histórico local o remoto")
        batch_queue: queue.Queue[list[AlpacaEvent] | None] = queue.Queue(
            maxsize=4
        )
        result_queue: queue.Queue[object] = queue.Queue(maxsize=1)

        def enqueue_batch(events: list[AlpacaEvent]) -> None:
            """Entrego un lote al consumidor respetando la presión."""

            while not self._stop_event.is_set():
                try:
                    batch_queue.put(events, timeout=0.5)
                    return
                except queue.Full:
                    continue
            raise InterruptedError("Descarga REPLAY detenida.")

        def download() -> None:
            """Descargo en segundo plano mientras consumo lotes."""

            try:
                result_queue.put(
                    get_historical_dataset(
                        self._api_key,
                        self._api_secret,
                        self._symbols,
                        self._feed,
                        self._run_id,
                        self._historical_path,
                        progress_callback=self._add_log,
                        event_callback=enqueue_batch,
                        stop_event=self._stop_event,
                    )
                )
            except BaseException as error:
                result_queue.put(error)
            finally:
                batch_queue.put(None)

        download_thread = threading.Thread(
            target=download,
            daemon=True,
            name=f"alpaca-download-{self._run_id}",
        )
        download_thread.start()
        streamed = False
        while True:
            batch = batch_queue.get()
            if batch is None:
                break
            streamed = True
            self._emit_replay_batch(batch)
        download_thread.join()
        result = result_queue.get()
        if isinstance(result, BaseException):
            raise result
        historical_path, historical_date, downloaded_streamed = result
        streamed = streamed or downloaded_streamed
        with self._lock:
            self._historical_date = historical_date
            self._add_log(
                f"REPLAY | histórico listo: {historical_date} | "
                f"archivo={historical_path.name}"
            )
            self._append_run_info_locked(
                "historical_complete",
                {
                    "data_file": historical_path.name,
                    "ordered_by_timestamp": True,
                },
            )
            self._append_state_locked()
        if not streamed:
            self._replay_cached_file(historical_path)
        self._finish("Completado", "replay_completed")

    def _emit_replay_batch(self, events: list[AlpacaEvent]) -> None:
        """Persisto y publico un lote cronológico con ritmo controlado."""

        for start in range(0, len(events), REPLAY_BATCH_SIZE):
            if self._stop_event.is_set():
                return
            batch = events[start:start + REPLAY_BATCH_SIZE]
            self._persist_event_batch(batch)
            if self._replay_speed > 0 and self._stop_event.wait(
                self._replay_speed
            ):
                return

    def _persist_event_batch(self, events: list[AlpacaEvent]) -> None:
        """Persisto un lote completo sin sincronizar una vez por evento."""

        if not events:
            return
        with self._lock:
            ensure_data_directory()
            stream_records = []
            for event in events:
                stream_records.append(
                    ("event", event.to_record(self._run_id))
                )
            self._events.extend(events)
            self._event_count += len(events)
            self._replay_position += len(events)
            self._status = "Reproduciendo"
            self._replay_batch_number += 1
            for _, record in stream_records:
                self._append_console_line_locked(
                    "alpaca",
                    event_from_record(record, mode="REPLAY").to_display_line(),
                )
            trade_count = sum(
                event.event_type == "TRADE" for event in events
            )
            quote_count = len(events) - trade_count
            self._add_log(
                f"REPLAY | lote emitido={self._replay_batch_number} | "
                f"operaciones={trade_count} | cotizaciones={quote_count} | "
                f"total_emitidos={self._event_count}"
            )
            self._append_state_locked()

    def _replay_cached_file(self, historical_path: Path) -> None:
        """Reproduzco una caché completa usando lotes acotados."""

        batch: list[AlpacaEvent] = []
        with historical_path.open(encoding="utf-8") as historical_file:
            for line in historical_file:
                if self._stop_event.is_set():
                    return
                batch.append(
                    event_from_record(json.loads(line), mode="REPLAY")
                )
                if len(batch) == REPLAY_BATCH_SIZE:
                    self._emit_replay_batch(batch)
                    batch = []
            if batch:
                self._emit_replay_batch(batch)

    async def _receive_events(self) -> None:
        """Autentico, suscribo y recibo los eventos de Alpaca."""

        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        self._set_async_resources(loop, task, None)
        uri = f"wss://stream.data.alpaca.markets/v2/{self._feed}"
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with connect(
            uri,
            open_timeout=20,
            ping_interval=20,
            ping_timeout=20,
            ssl=ssl_context,
        ) as websocket:
            self._set_async_resources(loop, task, websocket)
            await self._authenticate(websocket)
            self._add_log("LIVE | autenticación aceptada")
            await websocket.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "trades": self._symbols,
                        "quotes": self._symbols,
                    }
                )
            )
            self._set_status("Conectado", None)
            self._add_log(
                f"LIVE | suscripción activa: {', '.join(self._symbols)}"
            )
            while not self._stop_event.is_set():
                message = await websocket.recv()
                self._handle_message(message)

    async def _authenticate(self, websocket: ClientConnection) -> None:
        """Autentico el WebSocket antes de solicitar operaciones."""

        await websocket.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self._api_key,
                    "secret": self._api_secret,
                }
            )
        )
        while True:
            message = await websocket.recv()
            payload = self._decode_message(message)
            if any(
                item.get("T") == "success"
                and item.get("msg") == "authenticated"
                for item in payload
            ):
                return
            for item in payload:
                if item.get("T") == "error":
                    raise RuntimeError(self._format_server_error(item))

    def _handle_message(self, message: str | bytes) -> None:
        """Transformo y persisto cada operación recibida de Alpaca."""

        for payload in self._decode_message(message):
            event_type = payload.get("T")
            if event_type == "error":
                self._finish(
                    "Error",
                    "alpaca_error",
                    self._format_server_error(payload),
                )
                return
            if event_type not in {"t", "q"}:
                continue
            event = AlpacaEvent(
                timestamp=self._format_timestamp(payload.get("t")),
                symbol=str(payload.get("S", "-")),
                event_type="TRADE" if event_type == "t" else "QUOTE",
                price=self._number(payload.get("p"))
                if event_type == "t" else None,
                size=self._number(payload.get("s"))
                if event_type == "t" else None,
                bid_price=self._number(payload.get("bp"))
                if event_type == "q" else None,
                bid_size=self._number(payload.get("bs"))
                if event_type == "q" else None,
                ask_price=self._number(payload.get("ap"))
                if event_type == "q" else None,
                ask_size=self._number(payload.get("as"))
                if event_type == "q" else None,
                raw=payload,
            )
            self._persist_event(event)

    def _persist_event(
        self,
        event: AlpacaEvent,
        replay_position: int | None = None,
    ) -> None:
        """Agrego un evento al JSONL sin sobrescribir el historial."""

        with self._lock:
            record = event.to_record(self._run_id)
            with self._historical_path.open("a", encoding="utf-8") as event_file:
                event_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                event_file.flush()
                os.fsync(event_file.fileno())
            self._append_console_line_locked(
                "alpaca",
                event.to_display_line(),
            )
            self._events.append(event)
            self._event_count += 1
            if replay_position is not None:
                self._replay_position = replay_position
            self._append_state_locked()

    def _set_status(self, status: str, error: str | None) -> None:
        """Actualizo y persisto el estado transitorio de la conexión."""

        with self._lock:
            if self._finished:
                return
            self._status = status
            self._error = error
            self._append_state_locked()

    def _finish(
        self,
        status: str,
        end_reason: str,
        error: str | None = None,
    ) -> None:
        """Marco el RUN como terminado y libero su registro activo."""

        with self._lock:
            if self._finished:
                return
            self._finished = True
            self._status = status
            self._end_reason = end_reason
            self._error = error or self._error
            self._ended_at = get_utc_timestamp()
            self._stop_event.set()
            self._add_log(
                f"{self._mode} | finalizado: {status} | causa={end_reason}"
            )
            self._append_run_info_locked(
                "run_finished",
                {
                    "status": status,
                    "end_reason": end_reason,
                    "error": self._error,
                    "ended_at": self._ended_at,
                },
            )
            self._append_state_locked()
        if self._release_callback is not None:
            self._release_callback(self)

    def _append_state_locked(self) -> None:
        """Agrego el estado actual al único registro persistente del RUN."""

        self._append_run_info_locked(
            "state",
            {
                        "status": self._status,
                        "error": self._error,
                        "event_count": self._event_count,
                        "log_count": self._log_count,
                        "replay_position": self._replay_position,
                        "historical_date": self._historical_date,
                        "started_at": self._started_at,
                        "ended_at": self._ended_at,
                        "last_heartbeat": self._last_heartbeat_at,
                        "end_reason": self._end_reason,
                        "is_active": self.is_active(),
            },
        )

    def _append_console_line_locked(self, console_name: str, line: str) -> None:
        """Genero la numeración definitiva y la agrego al TXT correspondiente."""

        next_sequence = self._console_sequences[console_name] + 1
        self._console_sequences[console_name] = next_sequence
        console_paths = {
            "alpaca": self._log_alpaca_path,
            "log": self._log_handler_path,
            "spark": self._log_spark_path,
        }
        persisted_line = f"[#{next_sequence}] {line}"
        with console_paths[console_name].open(
            "a",
            encoding="utf-8",
        ) as console_file:
            console_file.write(persisted_line + "\n")
            console_file.flush()
            os.fsync(console_file.fileno())

    def _append_run_info_locked(
        self,
        record_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Persisto únicamente información estructurada de control del RUN."""

        self._sequence += 1
        record = {
            "record_type": record_type,
            "sequence": self._sequence,
            "run_id": self._run_id,
            "recorded_at": get_utc_timestamp(),
            **payload,
        }
        with self._run_info_path.open(
            "a",
            encoding="utf-8",
        ) as run_info_file:
            run_info_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            run_info_file.flush()
            os.fsync(run_info_file.fileno())

    def _add_log(self, message: str) -> None:
        """Agrego un mensaje operativo al TXT del RUN."""

        with self._lock:
            timestamp = format_presentation_timestamp(
                get_utc_timestamp(),
                source_timezone=timezone.utc,
            )
            self._append_console_line_locked("log", f"{timestamp} | {message}")
            self._log_count += 1
            self._append_state_locked()

    def _set_async_resources(
        self,
        loop: asyncio.AbstractEventLoop,
        task: asyncio.Task[None] | None,
        websocket: ClientConnection | None,
    ) -> None:
        """Conservo recursos asincrónicos para poder cancelarlos."""

        with self._lock:
            self._loop = loop
            self._task = task
            self._websocket = websocket

    def _clear_async_resources(self) -> None:
        """Libero las referencias a recursos asincrónicos finalizados."""

        with self._lock:
            self._loop = None
            self._task = None
            self._websocket = None

    def _join_threads(self) -> None:
        """Espero el cierre de los hilos sin bloquearse a sí mismos."""

        current_thread = threading.current_thread()
        for thread in (self._stream_thread,):
            if thread is not None and thread is not current_thread:
                thread.join(timeout=5)

    def _decode_message(self, message: str | bytes) -> list[dict[str, Any]]:
        """Decodifico los mensajes JSON recibidos desde Alpaca."""

        decoded = json.loads(message)
        return decoded if isinstance(decoded, list) else [decoded]

    def _format_server_error(self, payload: dict[str, Any]) -> str:
        """Interpreto el tipo de error informado por Alpaca."""

        message = str(payload.get("msg", "Error no especificado"))
        normalized_message = message.lower()
        code = payload.get("code")
        if code in {401, 402} or any(
            term in normalized_message
            for term in {"auth", "credential", "api key", "secret"}
        ):
            return "Autenticación Alpaca rechazada: credenciales inválidas."
        if any(
            term in normalized_message
            for term in {"connection limit", "too many", "already connected"}
        ):
            return (
                "Conexión Alpaca rechazada: límite de conexiones alcanzado."
            )
        if any(
            term in normalized_message
            for term in {"subscription", "symbol", "not authorized"}
        ):
            return f"Suscripción Alpaca rechazada: {message}"
        return f"Error de Alpaca: {message}"

    def _format_timestamp(self, value: Any) -> str:
        """Conservo el horario original del evento recibido."""

        return str(value) if value is not None else "-"

    def _number(self, value: Any) -> float | None:
        """Convierto un valor numérico recibido desde el WebSocket."""

        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _safe_error(self, error: Exception) -> str:
        """Devuelvo un error útil sin incluir secretos de autenticación."""

        message = str(error)
        if "opening handshake" in message.lower():
            return "No se pudo abrir el WebSocket de Alpaca a tiempo."
        return message.replace("key=", "key=[oculta]").replace(
            "secret=", "secret=[oculta]"
        )

def download_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    date_text: str,
    historical_path: Path,
    session_start: datetime,
    session_end: datetime,
    run_id: str,
    progress_callback: Callable[[str], None] | None = None,
    event_callback: Callable[[list[AlpacaEvent]], None] | None = None,
    stop_event: threading.Event | None = None,
) -> tuple[int, bool]:
    """Descargo páginas y completo el JSONL histórico del RUN."""

    temporary_prefix = f".tmp_{run_id}_"

    def cleanup_run_temporary_files() -> None:
        """Elimino solo los temporales pertenecientes al RUN actual."""

        if not TEMPORARY_DIRECTORY.exists():
            return
        for path in TEMPORARY_DIRECTORY.glob(f"{temporary_prefix}*"):
            if path.is_file():
                path.unlink(missing_ok=True)

    if ALL_MARKET_SYMBOL in symbols:
        raise RuntimeError(
            "El universo automático debe resolverse como S&P 500 antes "
            "de iniciar REPLAY."
        )
    market_symbols = symbols
    symbol_groups = [
        market_symbols[index:index + 100]
        for index in range(0, len(market_symbols), 100)
    ]
    if not symbol_groups:
        return 0, False
    cleanup_run_temporary_files()
    if progress_callback is not None:
        progress_callback(
            f"REPLAY | grupos de símbolos={len(symbol_groups)} | "
            f"símbolos consultados={len(market_symbols)}"
        )
    event_count = 0
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    def request_page(
        api_url: str,
        query: dict[str, str],
    ) -> dict[str, Any]:
        """Obtengo una página respetando límites y reintentos de Alpaca."""

        for attempt in range(HTTP_MAX_RETRIES):
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("Descarga REPLAY detenida.")
            url = f"{api_url}?{urllib.parse.urlencode(query)}"
            request = urllib.request.Request(
                url,
                headers={
                    "APCA-API-KEY-ID": api_key,
                    "APCA-API-SECRET-KEY": api_secret,
                },
            )
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=30,
                    context=ssl_context,
                ) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code in {401, 403}:
                    raise RuntimeError(
                        "Alpaca rechazó el acceso al histórico."
                    ) from error
                if error.code != 429 or attempt == HTTP_MAX_RETRIES - 1:
                    raise RuntimeError(
                        f"Alpaca devolvió HTTP {error.code}."
                    ) from error
                retry_after = error.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 2**attempt
                except ValueError:
                    delay = 2**attempt
                if stop_event is not None:
                    if stop_event.wait(min(delay, 60)):
                        raise InterruptedError(
                            "Descarga REPLAY detenida."
                        )
                else:
                    time.sleep(min(delay, 60))
        raise RuntimeError("Alpaca agotó los reintentos del histórico.")

    def download_group(
        group_index: int,
        symbol_group: list[str],
        window_start: datetime,
        window_end: datetime,
    ) -> tuple[int, list[tuple[int, Path]], int, int, int, int]:
        """Descargo un grupo y separo sus símbolos en temporales."""

        symbol_positions = {
            symbol: position
            for position, symbol in enumerate(symbol_group)
        }
        handles: dict[tuple[str, str], Any] = {}
        group_paths: dict[str, Path] = {}
        event_count = 0
        page_count = 0
        pages_with_trades = 0
        pages_with_quotes = 0
        try:
            for event_type, api_url, payload_key in (
                ("TRADE", HISTORICAL_TRADES_API_URL, "trades"),
                ("QUOTE", HISTORICAL_QUOTES_API_URL, "quotes"),
            ):
                next_page_token: str | None = None
                while True:
                    query = {
                        "symbols": ",".join(symbol_group),
                        "start": window_start.isoformat().replace(
                            "+00:00",
                            "Z",
                        ),
                        "end": (
                            (window_end - timedelta(seconds=1)).strftime(
                                "%Y-%m-%dT%H:%M:%S"
                            )
                            + ".999999999Z"
                        ),
                        "feed": feed,
                        "limit": "10000",
                        "sort": "asc",
                    }
                    if next_page_token:
                        query["page_token"] = next_page_token
                    payload = request_page(api_url, query)
                    page_count += 1
                    page_events = payload.get(payload_key, {})
                    if any(page_events.values()):
                        if event_type == "TRADE":
                            pages_with_trades += 1
                        else:
                            pages_with_quotes += 1
                    for symbol, source_events in page_events.items():
                        handle_key = (symbol, event_type)
                        if handle_key not in handles:
                            group_path = TEMPORARY_DIRECTORY / (
                                f"{temporary_prefix}"
                                f"{date_text}_{feed}_"
                                f"{window_start.strftime('%Y%m%dT%H%M%SZ')}_"
                                f"g{group_index:05d}_"
                                f"s{symbol_positions.get(symbol, 0):03d}_"
                                f"{event_type.lower()}.jsonl"
                            )
                            group_paths[f"{symbol}:{event_type}"] = group_path
                            handles[handle_key] = group_path.open(
                                "w",
                                encoding="utf-8",
                            )
                        group_file = handles[handle_key]
                        for source_event in source_events:
                            record = {
                                "symbol": symbol,
                                "event_type": event_type,
                                "timestamp": source_event.get("t"),
                                "price": source_event.get("p")
                                if event_type == "TRADE" else None,
                                "size": source_event.get("s")
                                if event_type == "TRADE" else None,
                                "bid_price": source_event.get("bp")
                                if event_type == "QUOTE" else None,
                                "bid_size": source_event.get("bs")
                                if event_type == "QUOTE" else None,
                                "ask_price": source_event.get("ap")
                                if event_type == "QUOTE" else None,
                                "ask_size": source_event.get("as")
                                if event_type == "QUOTE" else None,
                                "mode": "REPLAY",
                                "source": "alpaca_historical",
                                "raw": source_event,
                            }
                            group_file.write(
                                json.dumps(record, ensure_ascii=False)
                                + "\n"
                            )
                            event_count += 1
                    next_page_token = payload.get("next_page_token")
                    if not next_page_token:
                        break
        finally:
            for group_file in handles.values():
                group_file.close()
        return (
            group_index,
            [
                (group_index * 100 + position, path)
                for position, path in enumerate(group_paths.values())
            ],
            event_count,
            page_count,
            pages_with_trades,
            pages_with_quotes,
        )

    def record_key(record: dict[str, Any], group_index: int) -> tuple[str, str, str, int]:
        """Genero un desempate estable para timestamps iguales."""

        raw = record.get("raw")
        trade_id = raw.get("i", "") if isinstance(raw, dict) else ""
        return (
            str(record.get("timestamp", "")),
            str(record.get("symbol", "")),
            str(trade_id),
            group_index,
        )

    def merge_window(
        group_paths: list[tuple[int, Path]],
        historical_file: Any,
    ) -> tuple[int, list[AlpacaEvent]]:
        """Mezclo archivos temporales sin cargar la ventana completa."""

        handles: dict[int, Any] = {}
        heap: list[tuple[tuple[str, str, str, int], int, dict[str, Any]]] = []
        batch: list[AlpacaEvent] = []
        merged_count = 0
        try:
            for group_index, group_path in group_paths:
                handle = group_path.open(encoding="utf-8")
                handles[group_index] = handle
                line = handle.readline()
                if line:
                    record = json.loads(line)
                    heapq.heappush(
                        heap,
                        (record_key(record, group_index), group_index, record),
                    )
            while heap:
                _, group_index, record = heapq.heappop(heap)
                historical_file.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
                merged_count += 1
                if event_callback is not None:
                    batch.append(event_from_record(record, mode="REPLAY"))
                    if len(batch) == REPLAY_BATCH_SIZE:
                        event_callback(batch)
                        batch = []
                line = handles[group_index].readline()
                if line:
                    next_record = json.loads(line)
                    heapq.heappush(
                        heap,
                        (
                            record_key(next_record, group_index),
                            group_index,
                            next_record,
                        ),
                    )
        finally:
            for handle in handles.values():
                handle.close()
        return merged_count, batch

    try:
        with historical_path.open("a", encoding="utf-8") as historical_file:
            window_start = session_start
            day_end = session_end
            if progress_callback is not None:
                progress_callback(
                    f"REPLAY | sesión={date_text} | "
                    f"apertura="
                    f"{format_presentation_timestamp(session_start)} | "
                    f"cierre={format_presentation_timestamp(session_end)}"
                )
            while window_start < day_end:
                if stop_event is not None and stop_event.is_set():
                    raise InterruptedError("Descarga REPLAY detenida.")
                window_end = min(
                    window_start + timedelta(minutes=REPLAY_WINDOW_MINUTES),
                    day_end,
                )
                group_paths: list[tuple[int, Path]] = []
                window_downloaded = 0
                window_pages = 0
                window_pages_with_trades = 0
                window_pages_with_quotes = 0
                completed_groups = 0
                with ThreadPoolExecutor(
                    max_workers=MAX_HTTP_CONCURRENCY
                ) as executor:
                    futures = [
                        executor.submit(
                            download_group,
                            group_index,
                            symbol_group,
                            window_start,
                            window_end,
                        )
                        for group_index, symbol_group in enumerate(
                            symbol_groups
                        )
                    ]
                    for future in as_completed(futures):
                        (
                            group_index,
                            symbol_paths,
                            group_count,
                            group_pages,
                            group_pages_with_trades,
                            group_pages_with_quotes,
                        ) = future.result()
                        group_paths.extend(symbol_paths)
                        window_downloaded += group_count
                        window_pages += group_pages
                        window_pages_with_trades += group_pages_with_trades
                        window_pages_with_quotes += group_pages_with_quotes
                        completed_groups += 1
                        if progress_callback is not None:
                            progress_callback(
                                f"REPLAY | ventana="
                                f"{format_presentation_timestamp(window_start)}-"
                                f"{format_presentation_timestamp(window_end)} | "
                                f"grupos={completed_groups}/"
                                f"{len(symbol_groups)} | "
                                f"respuestas={window_pages} | "
                                f"páginas_con_operaciones="
                                f"{window_pages_with_trades} | "
                                f"páginas_con_cotizaciones="
                                f"{window_pages_with_quotes} | "
                                f"descargados={window_downloaded}"
                            )
                group_paths.sort()
                merged_count, pending_batch = merge_window(
                    group_paths,
                    historical_file,
                )
                event_count += merged_count
                if event_callback is not None and pending_batch:
                    event_callback(pending_batch)
                for _, group_path in group_paths:
                    group_path.unlink(missing_ok=True)
                historical_file.flush()
                os.fsync(historical_file.fileno())
                if progress_callback is not None:
                    progress_callback(
                        f"REPLAY | ventana="
                        f"{format_presentation_timestamp(window_start)}-"
                        f"{format_presentation_timestamp(window_end)} | "
                        f"respuestas={window_pages} | "
                        f"páginas_con_operaciones="
                        f"{window_pages_with_trades} | "
                        f"páginas_con_cotizaciones="
                        f"{window_pages_with_quotes} | "
                        f"descargados={window_downloaded} | "
                        f"emitidos_acumulados={event_count}"
                    )
                window_start = window_end
        if event_count == 0:
            return 0, False
        return event_count, event_callback is not None
    except Exception:
        raise
    finally:
        cleanup_run_temporary_files()

def ensure_data_directory() -> None:
    """Creo el directorio de registros cuando todavía no existe."""

    DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

def event_from_record(
    record: dict[str, Any],
    mode: str | None = None,
) -> AlpacaEvent:
    """Reconstruyo un evento persistido para mostrarlo en la consola."""

    return AlpacaEvent(
        timestamp=str(record.get("timestamp", "-")),
        symbol=str(record.get("symbol", "-")),
        event_type=str(record.get("event_type", "-")),
        price=to_number(record.get("price")),
        size=to_number(record.get("size")),
        mode=mode or str(record.get("mode", "LIVE")),
        console_sequence=(
            int(record["console_sequence"])
            if record.get("console_sequence") is not None
            else None
        ),
        bid_price=to_number(record.get("bid_price")),
        bid_size=to_number(record.get("bid_size")),
        ask_price=to_number(record.get("ask_price")),
        ask_size=to_number(record.get("ask_size")),
        raw=(
            dict(record.get("raw"))
            if isinstance(record.get("raw"), dict)
            else {}
        ),
    )

def get_run_file(run_id: str, file_kind: str) -> Path:
    """Obtengo la ruta de un archivo de un RUN nuevo."""

    suffixes = {
        "alpaca": "log_02_alpaca_stream.txt",
        "data": "data_alpaca.jsonl",
        "handler": "log_01_controller.txt",
        "info": "run_info.jsonl",
        "spark": "log_03_apache_spark.txt",
        "trades": "data_alpaca.jsonl",
    }
    path = DATA_DIRECTORY / f"{run_id}_{suffixes[file_kind]}"
    legacy_suffixes = {
        "alpaca": "log_alpaca.txt",
        "handler": "log_handler.txt",
        "spark": "log_04_apache_spark.txt",
    }
    if file_kind in legacy_suffixes and not path.exists():
        legacy_path = DATA_DIRECTORY / (
            f"{run_id}_{legacy_suffixes[file_kind]}"
        )
        if legacy_path.exists():
            return legacy_path
    return path

def read_console_delta(
    path: Path,
    byte_offset: int,
    record_limit: int | None,
) -> tuple[list[str], int, bool]:
    """Leo texto persistido desde un cursor de bytes sin reinterpretarlo."""

    lines: list[str] = []
    next_offset = max(0, byte_offset)
    if (
        record_limit is not None and record_limit <= 0
    ) or not path.exists():
        return lines, next_offset, False
    try:
        with path.open("rb") as console_file:
            console_file.seek(next_offset)
            while record_limit is None or len(lines) < record_limit:
                raw_line = console_file.readline()
                if not raw_line:
                    break
                next_offset = console_file.tell()
                lines.append(raw_line.decode("utf-8").rstrip("\r\n"))
            file_size = console_file.seek(0, os.SEEK_END)
    except (OSError, UnicodeDecodeError):
        return [], byte_offset, False
    return lines, next_offset, next_offset < file_size

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

def console_line_number(line: str) -> int:
    """Obtengo la numeración persistida de una línea de consola."""

    match = re.match(r"\[#(\d+)\]", line)
    return int(match.group(1)) if match else 0

def get_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    run_id: str,
    historical_path: Path,
    progress_callback: Callable[[str], None] | None = None,
    event_callback: Callable[[list[AlpacaEvent]], None] | None = None,
    stop_event: threading.Event | None = None,
) -> tuple[Path, str, bool]:
    """Obtengo un histórico validado sin crear metadata adicional."""

    date_text, session_start, session_end = get_latest_market_session(
        api_key,
        api_secret,
    )
    if progress_callback is not None:
        progress_callback(
            f"REPLAY | sesión bursátil seleccionada={date_text} | "
            f"apertura={format_presentation_timestamp(session_start)} | "
            f"cierre={format_presentation_timestamp(session_end)}"
        )
    cache_path = historical_file_path(date_text, symbols, feed)
    metadata = read_json(cache_path.with_suffix(".json"))
    if (
        metadata is not None
        and metadata.get("complete") is True
        and metadata.get("ordered_by_timestamp") is True
        and int(metadata.get("event_count", 0)) > 0
        and set(metadata.get("event_types", [])) >= {"TRADE", "QUOTE"}
        and metadata.get("regular_session") is True
        and metadata.get("session_start") == session_start.isoformat()
        and metadata.get("session_end") == session_end.isoformat()
        and cache_path.exists()
    ):
        historical_path.unlink(missing_ok=True)
        try:
            os.link(cache_path, historical_path)
        except OSError:
            if progress_callback is not None:
                progress_callback(
                    "REPLAY | caché válida no reutilizada sin duplicar; "
                    "se descargará el RUN"
                )
        else:
            if progress_callback is not None:
                progress_callback(
                    f"REPLAY | caché local reutilizada sin duplicar: "
                    f"{date_text} | eventos={metadata['event_count']}"
                )
            return historical_path, date_text, False
    event_count, streamed = download_historical_dataset(
        api_key,
        api_secret,
        symbols,
        feed,
        date_text,
        historical_path,
        session_start,
        session_end,
        run_id,
        progress_callback,
        event_callback,
        stop_event,
    )
    if event_count > 0:
        return historical_path, date_text, streamed
    raise RuntimeError(
        f"Alpaca no devolvió cotizaciones u operaciones para la sesión "
        f"{date_text}."
    )

def get_latest_metadata() -> dict[str, Any] | None:
    """Obtengo el estado del RUN más reciente desde su JSONL."""

    if not DATA_DIRECTORY.exists():
        return None
    new_paths = list(DATA_DIRECTORY.glob("*_run_info.jsonl"))
    legacy_paths = list(LEGACY_STREAMING_DIRECTORY.glob("*.jsonl"))
    candidates = new_paths + legacy_paths
    if candidates:
        latest_path = max(candidates, key=lambda path: path.stat().st_mtime_ns)
        if latest_path.parent == LEGACY_STREAMING_DIRECTORY:
            return read_streaming_state(latest_path)
        latest_stat = latest_path.stat()
        cache_key = str(latest_path)
        cached = _LATEST_METADATA_CACHE.get(cache_key)
        cache_stamp = (latest_stat.st_mtime_ns, latest_stat.st_size)
        if cached is not None and cached[:2] == cache_stamp:
            return dict(cached[2]) if cached[2] else None
        state = read_streaming_state(latest_path)
        _LATEST_METADATA_CACHE.clear()
        _LATEST_METADATA_CACHE[cache_key] = (
            latest_stat.st_mtime_ns,
            latest_stat.st_size,
            state,
        )
        if state is not None:
            return dict(state)
    metadata_paths = list(DATA_DIRECTORY.glob("*.json"))
    if not metadata_paths:
        return None
    latest_path = max(metadata_paths, key=lambda path: path.stat().st_mtime)
    return read_json(latest_path)

def get_latest_market_session(
    api_key: str,
    api_secret: str,
) -> tuple[str, datetime, datetime]:
    """Obtengo la última sesión completa desde el calendario de Alpaca."""

    now_new_york = datetime.now(NEW_YORK_TIMEZONE)
    query = urllib.parse.urlencode(
        {
            "start": (now_new_york.date() - timedelta(days=30)).isoformat(),
            "end": now_new_york.date().isoformat(),
        }
    )
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    authentication_error = False
    for calendar_api_url in CALENDAR_API_URLS:
        request = urllib.request.Request(
            f"{calendar_api_url}?{query}",
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=30,
                context=ssl_context,
            ) as response:
                calendar = json.load(response)
            break
        except urllib.error.HTTPError as error:
            if error.code in {401, 403}:
                authentication_error = True
                continue
            raise RuntimeError(
                f"Alpaca devolvió HTTP {error.code} al consultar el "
                "calendario."
            ) from error
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "No se pudo consultar el calendario de Alpaca."
            ) from error
    else:
        if authentication_error:
            raise RuntimeError(
                "Alpaca rechazó las credenciales para consultar el "
                "calendario."
            )
        raise RuntimeError("No se pudo consultar el calendario de Alpaca.")
    if not isinstance(calendar, list):
        raise RuntimeError("Alpaca devolvió un calendario inválido.")
    sessions: list[tuple[str, datetime, datetime]] = []
    for calendar_day in calendar:
        if not isinstance(calendar_day, dict):
            continue
        date_text = calendar_day.get("date")
        open_text = calendar_day.get("open")
        close_text = calendar_day.get("close")
        if not all(
            isinstance(value, str)
            for value in (date_text, open_text, close_text)
        ):
            continue
        try:
            session_start = datetime.strptime(
                f"{date_text} {open_text}",
                "%Y-%m-%d %H:%M",
            ).replace(tzinfo=NEW_YORK_TIMEZONE)
            session_end = datetime.strptime(
                f"{date_text} {close_text}",
                "%Y-%m-%d %H:%M",
            ).replace(tzinfo=NEW_YORK_TIMEZONE)
        except ValueError:
            continue
        session_end_utc = session_end.astimezone(timezone.utc)
        if session_end_utc <= datetime.now(timezone.utc):
            sessions.append(
                (
                    date_text,
                    session_start.astimezone(timezone.utc),
                    session_end_utc,
                )
            )
    if not sessions:
        raise RuntimeError(
            "Alpaca no devolvió una sesión bursátil completa disponible."
        )
    return max(sessions, key=lambda session: session[0])

def get_market_mode(api_key: str, api_secret: str) -> str:
    """Consulto en Alpaca si corresponde iniciar LIVE o REPLAY."""

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    authentication_error = False
    for market_clock_url in MARKET_CLOCK_URLS:
        request = urllib.request.Request(
            market_clock_url,
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=15,
                context=ssl_context,
            ) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code in {401, 403}:
                authentication_error = True
                continue
            raise RuntimeError(
                f"Alpaca devolvió HTTP {error.code} al consultar el mercado."
            ) from error
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "No se pudo consultar el estado del mercado en Alpaca."
            ) from error
        if not isinstance(payload, dict) or not isinstance(
            payload.get("is_open"), bool
        ):
            raise RuntimeError(
                "Alpaca devolvió una respuesta inválida para el estado del "
                "mercado."
            )
        return "LIVE" if payload["is_open"] else "REPLAY"
    if authentication_error:
        raise RuntimeError(
            "Alpaca rechazó la consulta del estado del mercado."
        )
    raise RuntimeError("No se pudo consultar el estado del mercado en Alpaca.")

def get_available_asset_symbols(
    api_key: str,
    api_secret: str,
) -> list[str]:
    """Obtengo los símbolos de acciones activos y tradables de Alpaca."""

    query = urllib.parse.urlencode(
        {"status": "active", "asset_class": "us_equity"}
    )
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    authentication_error = False
    for assets_api_url in ASSETS_API_URLS:
        request = urllib.request.Request(
            f"{assets_api_url}?{query}",
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=30,
                context=ssl_context,
            ) as response:
                assets = json.load(response)
            break
        except urllib.error.HTTPError as error:
            if error.code in {401, 403}:
                authentication_error = True
                continue
            raise RuntimeError(
                f"Alpaca no pudo devolver el mercado: HTTP {error.code}."
            ) from error
    else:
        if authentication_error:
            raise RuntimeError(
                "Alpaca rechazó las credenciales para consultar el mercado."
            )
        raise RuntimeError("No se pudo consultar el mercado en Alpaca.")
    symbols = {
        asset.get("symbol")
        for asset in assets
        if asset.get("tradable")
        and asset.get("status") == "active"
        and asset.get("symbol")
    }
    return sorted(symbols)

def get_sp500_ranked_symbols(
    api_key: str,
    api_secret: str,
    ranking: str,
) -> tuple[list[str], dict[str, Any]]:
    """Obtengo las 20 acciones más activas del S&P 500 desde Alpaca."""

    if ranking not in {"trades", "volume"}:
        raise ValueError(f"Ranking de Alpaca no soportado: {ranking}")
    sp500_symbols, universe_metadata = resolve_automatic_symbols(
        api_key,
        api_secret,
    )
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ranking_rows: list[tuple[str, float]] = []
    for start in range(0, len(sp500_symbols), 100):
        symbol_batch = sp500_symbols[start:start + 100]
        query = urllib.parse.urlencode(
            {"feed": "iex", "symbols": ",".join(symbol_batch)}
        )
        request = urllib.request.Request(
            f"{STOCK_SNAPSHOTS_URL}?{query}",
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=30,
                context=ssl_context,
            ) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code in {401, 403}:
                raise RuntimeError(
                    "Alpaca rechazó la consulta de snapshots del S&P 500."
                ) from error
            raise RuntimeError(
                f"Alpaca devolvió HTTP {error.code} al consultar snapshots."
            ) from error
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "No se pudieron consultar los snapshots del S&P 500."
            ) from error
        snapshot_data = (
            payload.get("snapshots", payload)
            if isinstance(payload, dict)
            else {}
        )
        for symbol, snapshot in snapshot_data.items():
            if not isinstance(snapshot, dict):
                continue
            daily_bar = snapshot.get("dailyBar") or snapshot.get(
                "daily_bar",
                {},
            )
            metric_key = "n" if ranking == "trades" else "v"
            metric = daily_bar.get(metric_key)
            if metric is not None:
                ranking_rows.append((str(symbol), float(metric)))
    symbols = [
        symbol
        for symbol, _ in sorted(
            ranking_rows,
            key=lambda item: (-item[1], item[0]),
        )[:RANKED_SYMBOL_COUNT]
    ]
    if len(symbols) != RANKED_SYMBOL_COUNT:
        raise RuntimeError(
            "Alpaca no devolvió 20 componentes del S&P 500 con datos "
            "suficientes para el ranking."
        )
    universe_metadata.update({
        "ranking": ranking,
        "symbols": symbols,
        "top": RANKED_SYMBOL_COUNT,
        "fetched_at": get_utc_timestamp(),
        "selection": "sp500_ranked",
    })
    return symbols, universe_metadata

def get_sp500_components() -> tuple[list[str], str]:
    """Obtengo los componentes actuales del S&P 500 desde su tabla pública."""

    request = urllib.request.Request(
        SP500_COMPONENTS_URL,
        headers={"User-Agent": "Apache-Big-Data-Lab/1.0"},
    )
    fetched_at = get_utc_timestamp()
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            source_updated_at = response.headers.get(
                "Last-Modified",
                fetched_at,
            )
            html = response.read().decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise RuntimeError(
            "No se pudo obtener la composición actual del S&P 500."
        ) from error
    parser = SP500TableParser()
    parser.feed(html)
    for table in parser.tables:
        for row_index, row in enumerate(table):
            headers = [cell.lower() for cell in row]
            if not any("symbol" in header for header in headers):
                continue
            symbol_index = next(
                index
                for index, header in enumerate(headers)
                if "symbol" in header
            )
            components = sorted(
                {
                    normalize_symbol(row_data[symbol_index])
                    for row_data in table[row_index + 1:]
                    if len(row_data) > symbol_index
                    and normalize_symbol(row_data[symbol_index])
                }
            )
            if components:
                return components, source_updated_at
    raise RuntimeError(
        "La fuente del S&P 500 no devolvió una tabla de símbolos válida."
    )

def normalize_symbol(symbol: str) -> str:
    """Normalizo un símbolo textual sin cambiar la clase de acción."""

    normalized = re.sub(r"\[[^]]*\]", "", str(symbol))
    return normalized.strip().upper().replace("/", "-")

def resolve_automatic_symbols(
    api_key: str,
    api_secret: str,
) -> tuple[list[str], dict[str, Any]]:
    """Valido los componentes del S&P 500 contra los activos de Alpaca."""

    components, source_updated_at = get_sp500_components()
    available_assets = get_available_asset_symbols(api_key, api_secret)
    asset_by_key = {
        normalize_symbol(symbol).replace(".", ""): symbol
        for symbol in available_assets
    }
    available = sorted(
        {
            asset_by_key[normalize_symbol(symbol).replace(".", "")]
            for symbol in components
            if normalize_symbol(symbol).replace(".", "") in asset_by_key
        }
    )
    available_keys = set(asset_by_key).intersection(
        normalize_symbol(symbol).replace(".", "")
        for symbol in components
    )
    unavailable = sorted(
        symbol
        for symbol in components
        if normalize_symbol(symbol).replace(".", "") not in available_keys
    )
    if not available:
        raise RuntimeError(
            "Ningún componente del S&P 500 está disponible en Alpaca."
        )
    return available, {
        "selection": "sp500",
        "source": SP500_COMPONENTS_URL,
        "index_source": SP500_INDEX_URL,
        "source_updated_at": source_updated_at,
        "fetched_at": get_utc_timestamp(),
        "component_count": len(components),
        "available_count": len(available),
        "unavailable_count": len(unavailable),
        "components": components,
        "available_symbols": available,
        "unavailable_symbols": unavailable,
    }

def get_recent_events(run_id: str | None) -> list[AlpacaEvent]:
    """Mantengo compatibilidad con llamadas externas sin cargar el historial."""

    if run_id is None:
        return []
    return read_event_delta(
        LEGACY_STREAMING_DIRECTORY / f"{run_id}.jsonl",
        0,
        256,
    )[0]

def read_event_delta(
    path: Path,
    byte_offset: int,
    record_limit: int | None,
) -> tuple[list[AlpacaEvent], int, bool]:
    """Leo eventos nuevos desde un cursor de bytes del JSONL."""

    events: list[AlpacaEvent] = []
    next_offset = max(0, byte_offset)
    if (
        record_limit is not None and record_limit <= 0
    ) or not path.exists():
        return events, next_offset, False
    try:
        with path.open("rb") as stream_file:
            stream_file.seek(next_offset)
            while record_limit is None or len(events) < record_limit:
                raw_line = stream_file.readline()
                if not raw_line:
                    break
                next_offset = stream_file.tell()
                try:
                    record = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if (
                    isinstance(record, dict)
                    and record.get("record_type") == "event"
                ):
                    events.append(event_from_record(record))
            file_size = stream_file.seek(0, os.SEEK_END)
    except OSError:
        return [], byte_offset, False
    return events, next_offset, next_offset < file_size

def read_log_delta(
    path: Path,
    byte_offset: int,
    record_limit: int | None,
) -> tuple[list[tuple[int | None, str]], int, bool]:
    """Leo logs nuevos desde un cursor de bytes del JSONL."""

    lines: list[tuple[int | None, str]] = []
    next_offset = max(0, byte_offset)
    if (
        record_limit is not None and record_limit <= 0
    ) or not path.exists():
        return lines, next_offset, False
    try:
        with path.open("rb") as stream_file:
            stream_file.seek(next_offset)
            while record_limit is None or len(lines) < record_limit:
                raw_line = stream_file.readline()
                if not raw_line:
                    break
                next_offset = stream_file.tell()
                try:
                    record = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if (
                    isinstance(record, dict)
                    and record.get("record_type") == "log"
                ):
                    lines.append(
                        (
                            int(record["console_sequence"])
                            if record.get("console_sequence") is not None
                            else None,
                            f"{record.get('recorded_at', '-')} | "
                            f"{record.get('message', '')}",
                        )
                    )
            file_size = stream_file.seek(0, os.SEEK_END)
    except OSError:
        return [], byte_offset, False
    return lines, next_offset, next_offset < file_size

def read_spark_delta(
    path: Path,
    byte_offset: int,
    record_limit: int | None,
) -> tuple[list[tuple[int, str]], int, bool, bool]:
    """Leo líneas Spark persistidas con su numeración original."""

    lines: list[tuple[int, str]] = []
    next_offset = max(0, byte_offset)
    schema_found = False
    if (
        record_limit is not None and record_limit <= 0
    ) or not path.exists():
        return lines, next_offset, False, schema_found
    try:
        with path.open("rb") as stream_file:
            stream_file.seek(next_offset)
            while record_limit is None or len(lines) < record_limit:
                raw_line = stream_file.readline()
                if not raw_line:
                    break
                next_offset = stream_file.tell()
                try:
                    record = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if (
                    not isinstance(record, dict)
                    or record.get("record_type") != "spark_log"
                ):
                    continue
                schema_found = True
                console_sequence = record.get("console_sequence")
                if console_sequence is None:
                    continue
                lines.append(
                    (
                        int(console_sequence),
                        str(record.get("message", "")),
                    )
                )
            file_size = stream_file.seek(0, os.SEEK_END)
    except OSError:
        return [], byte_offset, False, schema_found
    return lines, next_offset, next_offset < file_size, schema_found

def get_utc_timestamp() -> str:
    """Genero una marca temporal UTC reproducible para cada registro."""

    return datetime.now(timezone.utc).isoformat()

def build_run_stem(started_at: str) -> str:
    """Genero el identificador del RUN en la zona horaria argentina."""

    timestamp = datetime.fromisoformat(
        started_at.replace("Z", "+00:00")
    ).astimezone(PRESENTATION_TIMEZONE)
    return timestamp.strftime("%Y%m%d_%H%M%S")

def historical_file_path(
    date_text: str,
    symbols: list[str],
    feed: str,
) -> Path:
    """Genero una ruta estable para un histórico de símbolos y fecha."""

    symbol_text = "-".join(sorted(symbols))
    return HISTORICAL_DIRECTORY / (
        f"trades_{date_text}_{feed}_{symbol_text}.jsonl"
    )

def mark_interrupted_run() -> dict[str, Any] | None:
    """Reconozco un RUN activo que pertenecía a otro proceso Python."""

    metadata = get_latest_metadata()
    if metadata is None or not metadata.get("is_active"):
        return metadata
    metadata["status"] = "Incompleto"
    metadata["is_active"] = False
    metadata["end_reason"] = "process_restart"
    metadata["ended_at"] = get_utc_timestamp()
    run_id = metadata.get("run_id")
    streaming_file = (
        metadata.get("run_info_file")
        if int(metadata.get("console_schema_version", 0)) >= 3
        else metadata.get("streaming_file")
    )
    if isinstance(run_id, str) and isinstance(streaming_file, str):
        streaming_path = (
            DATA_DIRECTORY / streaming_file
            if int(metadata.get("console_schema_version", 0)) >= 3
            else LEGACY_STREAMING_DIRECTORY / streaming_file
        )
        sequence = int(metadata.get("sequence", 0))
        append_stream_record(
            streaming_path,
            run_id,
            sequence + 1,
            "state",
            {
                "status": "Incompleto",
                "is_active": False,
                "end_reason": "process_restart",
                "ended_at": metadata["ended_at"],
            },
        )
    return metadata

def read_json(path: Path) -> dict[str, Any] | None:
    """Leo la metadata de un RUN cuando el archivo es válido."""

    try:
        with path.open(encoding="utf-8") as metadata_file:
            content = json.load(metadata_file)
    except (OSError, json.JSONDecodeError):
        return None
    return content if isinstance(content, dict) else None

def append_stream_record(
    path: Path,
    run_id: str,
    sequence: int,
    record_type: str,
    payload: dict[str, Any],
) -> None:
    """Agrego un registro de recuperación sin reemplazar el JSONL."""

    record = {
        "record_type": record_type,
        "sequence": sequence,
        "run_id": run_id,
        "recorded_at": get_utc_timestamp(),
        **payload,
    }
    with path.open("a", encoding="utf-8") as stream_file:
        stream_file.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream_file.flush()
        os.fsync(stream_file.fileno())

def read_log_page(
    path: Path,
    cursor: int | str = "tail",
    page_size: int = 200,
) -> list[str]:
    """Leo una página de logs sin cargar el historial completo en memoria."""

    return [
        line
        for _, line in read_log_page_with_cursor(path, cursor, page_size)[0]
    ]

def read_log_page_with_cursor(
    path: Path,
    cursor: int | str = 0,
    page_size: int = 200,
) -> tuple[list[tuple[int, str]], int]:
    """Leo una página y devuelvo el cursor de su último registro."""

    if page_size <= 0 or not path.exists():
        return [], int(cursor) if isinstance(cursor, int) else 0
    if cursor == "tail":
        return read_tail_log_records(path, page_size)
    lines: list[tuple[int, str]] = []
    try:
        with path.open(encoding="utf-8") as stream_file:
            for line in stream_file:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("record_type") != "log":
                    continue
                sequence = int(record.get("sequence", 0))
                if isinstance(cursor, int) and sequence <= cursor:
                    continue
                log_line = (
                    f"{record.get('recorded_at', '-')} | "
                    f"{record.get('message', '')}"
                )
                lines.append((sequence, log_line))
                if len(lines) == page_size:
                    break
    except OSError:
        return [], int(cursor) if isinstance(cursor, int) else 0
    next_cursor = lines[-1][0] if lines else (
        int(cursor) if isinstance(cursor, int) else 0
    )
    return lines, next_cursor

def read_tail_log_records(
    path: Path,
    page_size: int,
) -> tuple[list[tuple[int, str]], int]:
    """Leo la página final sin recorrer el JSONL completo en cada refresco."""

    records: list[tuple[int, str]] = []
    try:
        with path.open("rb") as stream_file:
            position = stream_file.seek(0, os.SEEK_END)
            pending = b""
            while position > 0 and len(records) < page_size:
                block_size = min(8192, position)
                position -= block_size
                stream_file.seek(position)
                pending = stream_file.read(block_size) + pending
                raw_lines = pending.split(b"\n")
                pending = raw_lines.pop(0)
                for raw_line in reversed(raw_lines):
                    if not raw_line:
                        continue
                    try:
                        record = json.loads(raw_line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if record.get("record_type") != "log":
                        continue
                    sequence = int(record.get("sequence", 0))
                    records.append(
                        (
                            sequence,
                            f"{record.get('recorded_at', '-')} | "
                            f"{record.get('message', '')}",
                        )
                    )
                    if len(records) == page_size:
                        break
            if pending and len(records) < page_size:
                try:
                    record = json.loads(pending.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    record = {}
                if record.get("record_type") == "log":
                    records.append(
                        (
                            int(record.get("sequence", 0)),
                            f"{record.get('recorded_at', '-')} | "
                            f"{record.get('message', '')}",
                        )
                    )
    except OSError:
        return [], 0
    records.reverse()
    return records, records[-1][0] if records else 0

def read_streaming_state(path: Path) -> dict[str, Any] | None:
    """Reconstruyo el último estado sin materializar eventos ni logs."""

    if not path.exists():
        return None
    state: dict[str, Any] = {
        "run_id": path.stem,
        "symbols": [],
        "universe": {"selection": "manual", "symbols": []},
        "mode": "LIVE",
        "replay_speed": 1.0,
        "status": "Incompleto",
        "error": None,
        "event_count": 0,
        "log_count": 0,
        "replay_position": 0,
        "historical_date": None,
        "started_at": None,
        "ended_at": None,
        "last_heartbeat": None,
        "end_reason": None,
        "run_info_file": path.name,
        "data_file": None,
        "log_handler_file": None,
        "log_alpaca_file": None,
        "log_spark_file": None,
        "streaming_file": path.name,
        "historical_file": None,
        "console_schema_version": 0,
        "is_active": False,
        "sequence": 0,
        "logs": [],
    }
    started = False
    try:
        with path.open(encoding="utf-8") as stream_file:
            for line in stream_file:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                state["sequence"] = max(
                    state["sequence"],
                    int(record.get("sequence", 0)),
                )
                if record.get("run_id"):
                    state["run_id"] = record["run_id"]
                record_type = record.get("record_type")
                if record_type == "log":
                    state["log_count"] += 1
                if record_type == "run_started":
                    started = True
                    for key in (
                        "symbols",
                        "mode",
                        "replay_speed",
                        "started_at",
                        "data_file",
                        "trades_file",
                        "run_info_file",
                        "log_controller_file",
                        "log_handler_file",
                        "log_alpaca_file",
                        "log_spark_file",
                        "historical_file",
                        "streaming_file",
                        "universe",
                        "console_schema_version",
                    ):
                        if key in record:
                            state[key] = record[key]
                elif record_type == "state":
                    for key in (
                        "status",
                        "error",
                        "event_count",
                        "log_count",
                        "replay_position",
                        "historical_date",
                        "started_at",
                        "ended_at",
                        "last_heartbeat",
                        "end_reason",
                        "is_active",
                    ):
                        if key in record:
                            state[key] = record[key]
                elif record_type == "run_finished":
                    state["status"] = record.get("status", "Completado")
                    state["end_reason"] = record.get("end_reason")
                    state["error"] = record.get("error")
                    state["is_active"] = False
    except OSError:
        return None
    return state if started else None

def read_recent_events(run_id: str) -> list[AlpacaEvent]:
    """Leo un lote inicial sin materializar el historial completo."""

    data_path = get_run_file(run_id, "data")
    legacy_data_path = DATA_DIRECTORY / f"{run_id}_alpaca_trades.jsonl"
    if data_path.exists() or legacy_data_path.exists():
        return read_events_after(run_id, 0)[:256]
    return read_event_delta(
        LEGACY_STREAMING_DIRECTORY / f"{run_id}.jsonl",
        0,
        256,
    )[0]

def read_events_after(
    run_id: str,
    processed_count: int,
) -> list[AlpacaEvent]:
    """Leo únicamente los eventos posteriores al checkpoint persistido."""

    event_paths = [
        get_run_file(run_id, "data"),
        DATA_DIRECTORY / f"{run_id}_alpaca_trades.jsonl",
    ]
    new_events_path = next(
        (path for path in event_paths if path.exists()),
        None,
    )
    if new_events_path is not None:
        events: list[AlpacaEvent] = []
        event_index = 0
        try:
            with new_events_path.open(encoding="utf-8") as events_file:
                for line in events_file:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event_index >= processed_count:
                        events.append(event_from_record(record))
                    event_index += 1
        except OSError:
            return []
        return events
    events_path = LEGACY_STREAMING_DIRECTORY / f"{run_id}.jsonl"
    if not events_path.exists():
        return []
    events: list[AlpacaEvent] = []
    event_index = 0
    try:
        with events_path.open(encoding="utf-8") as stream_file:
            for line in stream_file:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    not isinstance(record, dict)
                    or record.get("record_type") != "event"
                ):
                    continue
                if event_index >= processed_count:
                    events.append(event_from_record(record))
                event_index += 1
    except OSError:
        return []
    return events

def read_spark_checkpoint(path: Path) -> dict[str, Any] | None:
    """Leo el último acumulado confirmado por Spark."""

    if path.name.endswith("_run_info.jsonl"):
        checkpoint = None
        try:
            with path.open(encoding="utf-8") as run_info_file:
                for line in run_info_file:
                    record = json.loads(line)
                    if record.get("record_type") == "spark_checkpoint":
                        checkpoint = {
                            key: value
                            for key, value in record.items()
                            if key not in {
                                "record_type",
                                "sequence",
                                "run_id",
                                "recorded_at",
                            }
                        }
        except (OSError, json.JSONDecodeError):
            return None
        return checkpoint
    if not path.exists():
        return None
    checkpoint: dict[str, Any] | None = None
    try:
        with path.open(encoding="utf-8") as stream_file:
            for line in stream_file:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("record_type") == "spark_checkpoint":
                    checkpoint = {
                        key: value
                        for key, value in record.items()
                        if key not in {
                            "record_type",
                            "sequence",
                            "run_id",
                            "recorded_at",
                        }
                    }
    except OSError:
        return None
    return checkpoint

def to_number(value: Any) -> float | None:
    """Convierto números persistidos a un valor seguro para la interfaz."""

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

_LATEST_METADATA_CACHE: dict[
    str,
    tuple[int, int, dict[str, Any] | None],
] = {}
