import certifi
import json
import queue
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from app import functions
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

_LATEST_METADATA_CACHE: dict[
    str,
    tuple[int, int, dict[str, Any] | None],
] = {}

ALPACA_API_KEY = "PKJQMFWVYH6PEXUKK34QAABOUW"
ALPACA_SECRET_KEY = "5xBskyt9ofjunrzuCa77RDwjP5n5fGCDDv6XrfxqzAyk"
ALL_MARKET_SYMBOL = "*"
ASSETS_API_URL = "https://paper-api.alpaca.markets/v2/assets"
CALENDAR_API_URL = "https://paper-api.alpaca.markets/v2/calendar"
HISTORICAL_QUOTES_API_URL = "https://data.alpaca.markets/v2/stocks/quotes"
HISTORICAL_TRADES_API_URL = "https://data.alpaca.markets/v2/stocks/trades"
HTTP_MAX_RETRIES = 5
MAX_HTTP_CONCURRENCY = 6
NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")
RANKED_SYMBOL_COUNT = 20
REPLAY_BATCH_SIZE = 100
REPLAY_WINDOW_MINUTES = 1
SP500_COMPONENTS_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
STOCK_SNAPSHOTS_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

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
        self.console_sequence = console_sequence

    def to_display_line(self) -> str:
        """Convierto el evento a una línea segura para mostrarlo."""

        timestamp = format_presentation_timestamp(
            self.timestamp,
            source_timezone=timezone.utc,
        )
        if self.event_type == "QUOTE":
            return (
                f"{timestamp} | {self.symbol} | QUOTE | bid={self._format_number(self.bid_price)} | bid_size={self._format_number(self.bid_size)} | ask={self._format_number(self.ask_price)} | ask_size={self._format_number(self.ask_size)}"
            )
        price = self._format_number(self.price, decimals=4)
        size = self._format_number(self.size)
        return f"{timestamp} | {self.symbol} | {self.event_type} | {price} | {size}"

    def _format_number(
        self,
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
        """Convierto el evento al registro que se publica en Kafka."""

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
        replay_speed: float = 1.0,
        release_callback: Callable[["AlpacaStreamingService"], None]
        | None = None,
        publish_events_callback: Callable[
            [str, list[dict[str, Any]], threading.Event | None],
            None,
        ] | None = None,
        append_controller_log_callback: Callable[[str, str, Any], None]
        | None = None,
        append_run_record_callback: Callable[
            [str, str, dict[str, Any]],
            None,
        ] | None = None,
        append_run_state_callback: Callable[[Any], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._append_controller_log_callback = append_controller_log_callback
        self._append_run_record_callback = append_run_record_callback
        self._append_run_state_callback = append_run_state_callback
        self._download_thread: threading.Thread | None = None
        self._end_reason: str | None = None
        self._ended_at: str | None = None
        self._error: str | None = None
        self._event_count = 0
        self._events: deque[AlpacaEvent] = deque()
        self._feed = feed
        self._finished = False
        self._historical_date: str | None = None
        self._kafka_published_count = 0
        self._last_heartbeat_at: str | None = None
        self._last_heartbeat_monotonic: float | None = None
        self._lock = threading.RLock()
        self._market_timestamp: str | None = None
        self._pipeline_ready = threading.Event()
        self._publish_events_callback = publish_events_callback
        self._release_callback = release_callback
        self._replay_batch_number = 0
        self._replay_position = 0
        self._replay_speed = replay_speed
        self._run_id = run_id
        self._run_stem = run_stem
        self._started_at = started_at
        self._status = "Listo para ejecutar"
        self._stop_event = threading.Event()
        self._stream_thread: threading.Thread | None = None
        self._symbols = symbols
        self._universe_metadata = universe_metadata
        self._log_alpaca_path = (
            DATA_DIRECTORY / "runs" / f"{run_stem}_02_alpaca_stream_log.txt"
        )
        ensure_data_directory()
        if any(
            path.exists()
            for path in (
                self._log_alpaca_path,
            )
        ):
            raise FileExistsError(f"El RUN ya existe: {run_stem}")

    def start(self) -> bool:
        """Inicio los hilos de streaming y supervisión del RUN."""

        with self._lock:
            if self.is_active():
                return False
            self._status = "Descargando histórico"
            self._last_heartbeat_at = self._started_at
            self._last_heartbeat_monotonic = None
            self._append_run_record_callback(
                self._run_id,
                "run_started",
                {
                    "symbols": list(self._symbols),
                    "feed": self._feed,
                    "replay_speed": self._replay_speed,
                    "started_at": self._started_at,
                    "universe": self._universe_metadata,
                },
            )
            self._add_log("DATA | RUN iniciado")
            if self._universe_metadata.get("selection") == "sp500":
                self._add_log(
                    "DATA | S&P 500 | componentes={components} | disponibles={available} | no_disponibles={missing}"
                    .format(
                        components=self._universe_metadata["component_count"],
                        available=self._universe_metadata["available_count"],
                        missing=self._universe_metadata["unavailable_count"],
                    )
                )
                self._add_log(
                    "DATA | S&P 500 | fuente={source} | actualizada={updated} | consultada={fetched}".format(
                        source=self._universe_metadata["source"],
                        updated=self._universe_metadata["source_updated_at"],
                        fetched=self._universe_metadata["fetched_at"],
                    )
                )
                self._append_run_record_callback(
                    self._run_id,
                    "universe",
                    self._universe_metadata,
                )
            self._append_run_state_callback(self)
            self._stream_thread = threading.Thread(
                target=self._run_stream,
                daemon=True,
                name=f"alpaca-stream-{self._run_id}",
            )
            self._stream_thread.start()
            self._add_log("DATA | worker de supervisión iniciado")
            self._append_run_state_callback(self)
        return True

    def heartbeat(self, run_id: str) -> bool:
        """Registro una señal válida proveniente de la sesión visual."""

        with self._lock:
            if run_id != self._run_id or not self.is_active():
                return False
            self._last_heartbeat_monotonic = time.monotonic()
            self._last_heartbeat_at = get_utc_timestamp()
            self._append_run_state_callback(self)
        return True

    def heartbeat_elapsed(self) -> float:
        """Devuelvo los segundos desde el último heartbeat válido."""

        with self._lock:
            if self._last_heartbeat_monotonic is None:
                return 0.0
            return time.monotonic() - self._last_heartbeat_monotonic

    def stop(self, end_reason: str) -> None:
        """Detengo el RUN y solicito el cierre de sus hilos."""

        self.request_stop(end_reason)
        self._finish("Detenido", end_reason)
        self._join_threads()

    def request_stop(self, end_reason: str) -> None:
        """Solicito el cierre inmediato sin esperar los hilos de trabajo."""

        with self._lock:
            if self._finished or self._stop_event.is_set():
                return
            self._status = "Finalizando"
            self._end_reason = end_reason
            self._stop_event.set()
            self._pipeline_ready.set()
            self._add_log(f"DATA | detención solicitada: {end_reason}")
            self._append_run_state_callback(self)

    def fail(self, error: str) -> None:
        """Finalizo el RUN con error y libero sus recursos."""

        self._finish("Error", "startup_error", error)

    def release_pipeline(self) -> None:
        """Habilito la publicación cuando Kafka y Spark están preparados."""

        self._pipeline_ready.set()

    def is_active(self) -> bool:
        """Indico si el RUN sigue aceptando heartbeats."""

        return not self._finished and self._status in {
            "Descargando histórico",
            "Reproduciendo",
            "Finalizando",
        }

    def is_running(self) -> bool:
        """Indico si el hilo de datos todavía está activo."""

        return bool(self._stream_thread and self._stream_thread.is_alive())

    def get_snapshot(self) -> dict[str, Any]:
        """Devuelvo el estado actual del RUN sin exponer credenciales."""

        with self._lock:
            return {
                "run_id": self._run_id,
                "symbols": list(self._symbols),
                "universe": self._universe_metadata,
                "replay_speed": self._replay_speed,
                "status": self._status,
                "error": self._error,
                "event_count": self._event_count,
                "market_timestamp": self._market_timestamp,
                "replay_position": self._replay_position,
                "historical_date": self._historical_date,
                "logs": [],
                "started_at": self._started_at,
                "ended_at": self._ended_at,
                "last_heartbeat": self._last_heartbeat_at,
                "end_reason": self._end_reason,
                "is_active": self.is_active(),
            }

    def _run_stream(self) -> None:
        """Ejecuto la descarga y reproducción histórica en un hilo propio."""

        try:
            self._add_log("DATA | worker de datos iniciado")
            self._run_replay()
        except Exception as error:
            if not self._stop_event.is_set():
                self._finish(
                    "Error",
                    "replay_error",
                    self._safe_error(error),
                )
        finally:
            if self._stop_event.is_set():
                self._finish("Detenido", self._end_reason or "manual_stop")

    def _run_replay(self) -> None:
        """Descargo y reproduzco eventos históricos de forma progresiva."""

        self._add_log("DATA | buscando histórico local o remoto")
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
            raise InterruptedError("Descarga histórica detenida.")

        def download() -> None:
            """Descargo en segundo plano mientras consumo lotes."""

            try:
                result_queue.put(
                    get_historical_dataset(
                        self._api_key,
                        self._api_secret,
                        self._symbols,
                        self._feed,
                        progress_callback=self._add_log,
                        event_callback=enqueue_batch,
                        historical_date_callback=self._set_historical_date,
                        stop_event=self._stop_event,
                    )
                )
            except BaseException as error:
                result_queue.put(error)
            finally:
                if self._stop_event.is_set():
                    try:
                        batch_queue.put_nowait(None)
                    except queue.Full:
                        pass
                else:
                    batch_queue.put(None)

        download_thread = threading.Thread(
            target=download,
            daemon=True,
            name=f"alpaca-download-{self._run_id}",
        )
        self._download_thread = download_thread
        download_thread.start()
        while True:
            try:
                batch = batch_queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    return
                continue
            if batch is None or self._stop_event.is_set():
                break
            self._emit_replay_batch(batch)
        if self._stop_event.is_set():
            return
        download_thread.join()
        result = result_queue.get()
        if isinstance(result, BaseException):
            raise result
        historical_date = result
        with self._lock:
            self._historical_date = historical_date
            self._add_log(
                f"DATA | histórico listo: {historical_date} | eventos_en_memoria"
            )
            self._append_run_record_callback(
                self._run_id,
                "historical_complete",
                {
                    "ordered_by_timestamp": True,
                },
            )
            self._append_run_state_callback(self)
        self._finish("Completado", "replay_completed")

    def _set_historical_date(self, historical_date: str) -> None:
        """Conservo la fecha bursátil seleccionada antes de descargar."""

        with self._lock:
            if self._finished:
                return
            self._historical_date = historical_date
            self._append_run_state_callback(self)

    def _emit_replay_batch(self, events: list[AlpacaEvent]) -> None:
        """Preparo y publico un lote cronológico con ritmo controlado."""

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
        """Publico un lote completo sin sincronizar una vez por evento."""

        if not events:
            return
        while not self._pipeline_ready.wait(0.1):
            if self._stop_event.is_set():
                return
        with self._lock:
            if self._stop_event.is_set():
                return
            ensure_data_directory()
            stream_records = [
                event.to_record(self._run_id)
                for event in events
            ]
            self._events.extend(events)
            self._event_count += len(events)
            self._market_timestamp = events[-1].timestamp
            self._replay_position += len(events)
            self._status = "Reproduciendo"
            self._replay_batch_number += 1
            for record in stream_records:
                functions.append_console_log(
                    self._run_id,
                    "alpaca",
                    event_from_record(record).to_display_line(),
                )
            trade_count = sum(
                event.event_type == "TRADE" for event in events
            )
            quote_count = len(events) - trade_count
            self._add_log(
                f"DATA | lote emitido={self._replay_batch_number} | operaciones={trade_count} | cotizaciones={quote_count} | total_emitidos={self._event_count}"
            )
            self._append_run_state_callback(self)
        if (
            self._publish_events_callback is not None
            and not self._stop_event.is_set()
        ):
            self._publish_events_callback(
                self._run_id,
                stream_records,
                self._stop_event,
            )
            with self._lock:
                self._kafka_published_count += len(stream_records)

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
                f"DATA | finalizado: {status} | causa={end_reason}"
            )
            self._append_run_record_callback(
                self._run_id,
                "run_finished",
                {
                    "status": status,
                    "end_reason": end_reason,
                    "error": self._error,
                    "ended_at": self._ended_at,
                },
            )
            self._append_run_state_callback(self)
        if self._release_callback is not None:
            self._release_callback(self)

    def _add_log(self, message: str) -> None:
        """Informo al Controller un mensaje operativo de Alpaca."""

        with self._lock:
            if self._stop_event.is_set() and not message.startswith("DATA | finalizado:"):
                return
            if self._append_controller_log_callback is not None:
                self._append_controller_log_callback(
                    self._run_id,
                    message,
                    self,
                )

    def _join_threads(self) -> None:
        """Espero el cierre de los hilos sin bloquearse a sí mismos."""

        current_thread = threading.current_thread()
        for thread in (self._stream_thread, self._download_thread):
            if thread is not None and thread is not current_thread:
                thread.join(timeout=3)

    def _safe_error(self, error: Exception) -> str:
        """Devuelvo un error útil sin incluir secretos de autenticación."""

        message = str(error)
        return message.replace("key=", "key=[oculta]").replace(
            "secret=", "secret=[oculta]"
        )

def download_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    date_text: str,
    session_start: datetime,
    session_end: datetime,
    progress_callback: Callable[[str], None] | None = None,
    event_callback: Callable[[list[AlpacaEvent]], None] | None = None,
    stop_event: threading.Event | None = None,
) -> tuple[int, bool]:
    """Descargo páginas y entrego eventos ordenados directamente al pipeline."""

    if ALL_MARKET_SYMBOL in symbols:
        raise RuntimeError(
            "El universo automático debe resolverse como S&P 500 antes de iniciar la descarga histórica."
        )
    market_symbols = symbols
    symbol_groups = [
        market_symbols[index:index + 100]
        for index in range(0, len(market_symbols), 100)
    ]
    if not symbol_groups:
        return 0, False
    if progress_callback is not None:
        progress_callback(
            f"DATA | grupos de símbolos={len(symbol_groups)} | símbolos consultados={len(market_symbols)}"
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
                        raise InterruptedError("Descarga histórica detenida.")
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
                            "Descarga histórica detenida."
                        )
                else:
                    time.sleep(min(delay, 60))
        raise RuntimeError("Alpaca agotó los reintentos del histórico.")

    def download_group(
        group_index: int,
        symbol_group: list[str],
        window_start: datetime,
        window_end: datetime,
    ) -> tuple[int, list[dict[str, Any]], int, int, int, int]:
        """Descargo un grupo y conservo sus eventos en memoria."""

        group_records: list[dict[str, Any]] = []
        event_count = 0
        page_count = 0
        responses_with_trades = 0
        responses_with_quotes = 0
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
                        responses_with_trades += 1
                    else:
                        responses_with_quotes += 1
                for symbol, source_events in page_events.items():
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
                            "source": "alpaca_historical",
                            "raw": source_event,
                        }
                        group_records.append(record)
                        event_count += 1
                next_page_token = payload.get("next_page_token")
                if not next_page_token:
                    break
        return (
            group_index,
            group_records,
            event_count,
            page_count,
            responses_with_trades,
            responses_with_quotes,
        )

    def record_key(
        record: dict[str, Any],
        group_index: tuple[int, int],
    ) -> tuple[str, str, str, tuple[int, int]]:
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
        group_records: list[tuple[int, list[dict[str, Any]]]],
    ) -> tuple[int, list[AlpacaEvent]]:
        """Mezclo en memoria los eventos ordenados de la ventana."""

        ordered_records = [
            (
                record_key(record, (group_index, position)),
                record,
            )
            for group_index, records in group_records
            for position, record in enumerate(records)
        ]
        ordered_records.sort(key=lambda item: item[0])
        batch: list[AlpacaEvent] = []
        for _, record in ordered_records:
            if event_callback is not None:
                batch.append(event_from_record(record))
                if len(batch) == REPLAY_BATCH_SIZE:
                    event_callback(batch)
                    batch = []
        return len(ordered_records), batch

    window_start = session_start
    day_end = session_end
    if progress_callback is not None:
        progress_callback(
            f"DATA | sesión={date_text} | apertura={format_presentation_timestamp(session_start)} | cierre={format_presentation_timestamp(session_end)}"
        )
    while window_start < day_end:
        if stop_event is not None and stop_event.is_set():
            raise InterruptedError("Descarga histórica detenida.")
        window_end = min(
            window_start + timedelta(minutes=REPLAY_WINDOW_MINUTES),
            day_end,
        )
        group_records: list[tuple[int, list[dict[str, Any]]]] = []
        window_downloaded = 0
        window_responses = 0
        window_responses_with_trades = 0
        window_responses_with_quotes = 0
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
                for group_index, symbol_group in enumerate(symbol_groups)
            ]
            for future in as_completed(futures):
                (
                    group_index,
                    records,
                    group_count,
                    group_responses,
                    group_responses_with_trades,
                    group_responses_with_quotes,
                ) = future.result()
                group_records.append((group_index, records))
                window_downloaded += group_count
                window_responses += group_responses
                window_responses_with_trades += group_responses_with_trades
                window_responses_with_quotes += group_responses_with_quotes
                completed_groups += 1
                if progress_callback is not None:
                    progress_callback(
            f"DATA | ventana={format_presentation_timestamp(window_start)}-{format_presentation_timestamp(window_end)} | grupos={completed_groups}/{len(symbol_groups)} | respuestas={window_responses} | páginas_con_operaciones={window_responses_with_trades} | páginas_con_cotizaciones={window_responses_with_quotes} | descargados={window_downloaded}"
                    )
        group_records.sort(key=lambda item: item[0])
        merged_count, pending_batch = merge_window(group_records)
        event_count += merged_count
        if event_callback is not None and pending_batch:
            event_callback(pending_batch)
        if progress_callback is not None:
            progress_callback(
                f"DATA | ventana={format_presentation_timestamp(window_start)}-{format_presentation_timestamp(window_end)} | respuestas={window_responses} | páginas_con_operaciones={window_responses_with_trades} | páginas_con_cotizaciones={window_responses_with_quotes} | descargados={window_downloaded} | emitidos_acumulados={event_count}"
            )
        window_start = window_end
    if event_count == 0:
        return 0, False
    return event_count, event_callback is not None

def ensure_data_directory() -> None:
    """Creo el directorio de registros cuando todavía no existe."""

    DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)
    (DATA_DIRECTORY / "runs").mkdir(parents=True, exist_ok=True)

def event_from_record(
    record: dict[str, Any],
) -> AlpacaEvent:
    """Reconstruyo un evento persistido para mostrarlo en la consola."""

    return AlpacaEvent(
        timestamp=str(record.get("timestamp", "-")),
        symbol=str(record.get("symbol", "-")),
        event_type=str(record.get("event_type", "-")),
        price=to_number(record.get("price")),
        size=to_number(record.get("size")),
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

def get_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    progress_callback: Callable[[str], None] | None = None,
    event_callback: Callable[[list[AlpacaEvent]], None] | None = None,
    historical_date_callback: Callable[[str], None] | None = None,
    stop_event: threading.Event | None = None,
) -> str:
    """Obtengo un histórico validado y lo entrego directamente al pipeline."""

    date_text, session_start, session_end = get_latest_market_session(
        api_key,
        api_secret,
    )
    if historical_date_callback is not None:
        historical_date_callback(date_text)
    if progress_callback is not None:
        progress_callback(
            f"DATA | sesión bursátil seleccionada={date_text} | apertura={format_presentation_timestamp(session_start)} | cierre={format_presentation_timestamp(session_end)}"
        )
    event_count, _ = download_historical_dataset(
        api_key,
        api_secret,
        symbols,
        feed,
        date_text,
        session_start,
        session_end,
        progress_callback,
        event_callback,
        stop_event,
    )
    if event_count > 0:
        return date_text
    raise RuntimeError(
        f"Alpaca no devolvió cotizaciones u operaciones para la sesión {date_text}."
    )

def get_latest_metadata() -> dict[str, Any] | None:
    """Obtengo el estado del RUN más reciente desde su JSONL."""

    if not DATA_DIRECTORY.exists():
        return None
    candidates = list(
        (DATA_DIRECTORY / "runs").glob(
            "*_01_python_controller_info.jsonl"
        )
    )
    if candidates:
        latest_path = max(candidates, key=lambda path: path.stat().st_mtime_ns)
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
    request = urllib.request.Request(
        f"{CALENDAR_API_URL}?{query}",
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
    except urllib.error.HTTPError as error:
        if error.code in {401, 403}:
            raise RuntimeError(
                "Alpaca rechazó las credenciales para consultar el calendario."
            ) from error
        raise RuntimeError(
            f"Alpaca devolvió HTTP {error.code} al consultar el calendario."
        ) from error
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "No se pudo consultar el calendario de Alpaca."
        ) from error
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

def get_available_asset_symbols(
    api_key: str,
    api_secret: str,
) -> list[str]:
    """Obtengo los símbolos de acciones activos y tradables de Alpaca."""

    query = urllib.parse.urlencode(
        {"status": "active", "asset_class": "us_equity"}
    )
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(
        f"{ASSETS_API_URL}?{query}",
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
    except urllib.error.HTTPError as error:
        if error.code in {401, 403}:
            raise RuntimeError(
                "Alpaca rechazó las credenciales para consultar el mercado."
            ) from error
        raise RuntimeError(
            f"Alpaca no pudo devolver el mercado: HTTP {error.code}."
        ) from error
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "No se pudo consultar el mercado en Alpaca."
        ) from error
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
            "Alpaca no devolvió 20 componentes del S&P 500 con datos suficientes para el ranking."
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
        "source_updated_at": source_updated_at,
        "fetched_at": get_utc_timestamp(),
        "component_count": len(components),
        "available_count": len(available),
        "unavailable_count": len(unavailable),
        "components": components,
        "available_symbols": available,
        "unavailable_symbols": unavailable,
    }

def get_utc_timestamp() -> str:
    """Genero una marca temporal UTC reproducible para cada registro."""

    return datetime.now(timezone.utc).isoformat()

def build_run_stem(started_at: str) -> str:
    """Genero el identificador del RUN en la zona horaria argentina."""

    timestamp = datetime.fromisoformat(
        started_at.replace("Z", "+00:00")
    ).astimezone(PRESENTATION_TIMEZONE)
    return timestamp.strftime("%Y%m%d_%H%M%S")

def read_json(path: Path) -> dict[str, Any] | None:
    """Leo la metadata de un RUN cuando el archivo es válido."""

    try:
        with path.open(encoding="utf-8") as metadata_file:
            content = json.load(metadata_file)
    except (OSError, json.JSONDecodeError):
        return None
    return content if isinstance(content, dict) else None

def read_streaming_state(path: Path) -> dict[str, Any] | None:
    """Reconstruyo el último estado sin materializar eventos ni logs."""

    if not path.exists():
        return None
    state: dict[str, Any] = {
        "run_id": path.stem,
        "symbols": [],
        "universe": {"selection": "manual", "symbols": []},
        "replay_speed": 1.0,
        "status": "Incompleto",
        "error": None,
        "event_count": 0,
        "log_count": 0,
        "market_timestamp": None,
        "replay_position": 0,
        "historical_date": None,
        "started_at": None,
        "ended_at": None,
        "last_heartbeat": None,
        "end_reason": None,
        "info_controller_file": path.name,
        "log_handler_file": None,
        "log_alpaca_file": None,
        "log_spark_file": None,
        "log_pandas_file": None,
        "log_postgresql_file": None,
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
                        "replay_speed",
                        "started_at",
                        "info_controller_file",
                        "log_controller_file",
                        "log_handler_file",
                        "log_alpaca_file",
                        "log_spark_file",
                        "log_pandas_file",
                        "log_postgresql_file",
                        "universe",
                    ):
                        if key in record:
                            state[key] = record[key]
                elif record_type == "state":
                    for key in (
                        "status",
                        "error",
                        "event_count",
                        "log_count",
                        "market_timestamp",
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

def to_number(value: Any) -> float | None:
    """Convierto números persistidos a un valor seguro para la interfaz."""

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
