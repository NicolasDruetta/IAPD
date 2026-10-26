"""Gestión del ciclo de vida del streaming WebSocket de Alpaca."""

import asyncio
import atexit
import certifi
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from websockets.asyncio.client import ClientConnection, connect
from zoneinfo import ZoneInfo

ALPACA_API_KEY = "PKJQMFWVYH6PEXUKK34QAABOUW"
ALPACA_SECRET_KEY = "5xBskyt9ofjunrzuCa77RDwjP5n5fGCDDv6XrfxqzAyk"
DATA_DIRECTORY = Path(__file__).resolve().parent.parent / "data" / "streaming"
HISTORICAL_DIRECTORY = (
    Path(__file__).resolve().parent.parent / "data" / "historical"
)
HISTORICAL_API_URL = "https://data.alpaca.markets/v2/stocks/trades"
MARKET_CLOCK_URLS = (
    "https://api.alpaca.markets/v2/clock",
    "https://paper-api.alpaca.markets/v2/clock",
)
HEARTBEAT_TIMEOUT_SECONDS = 30
SUPERVISOR_INTERVAL_SECONDS = 1
VISIBLE_EVENT_LIMIT = 100
LOG_LIMIT = 200
REPLAY_SPEEDS = {
    "1 evento por segundo": 1.0,
    "10 eventos por segundo": 0.1,
    "100 eventos por segundo": 0.01,
    "Máxima velocidad controlada": 0.0,
}
NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")

class AlpacaEvent:
    """Represento un evento financiero recibido desde Alpaca."""

    timestamp: str
    symbol: str
    event_type: str
    price: float | None
    size: float | None

    def __init__(
        self,
        timestamp: str,
        symbol: str,
        event_type: str,
        price: float | None,
        size: float | None,
        mode: str = "LIVE",
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.event_type = event_type
        self.price = price
        self.size = size
        self.mode = mode

    def to_display_line(self) -> str:
        """Convierto el evento a una línea segura para mostrarlo."""

        price = "-" if self.price is None else f"{self.price:.4f}"
        size = "-" if self.size is None else f"{self.size:g}"
        return (
            f"{self.timestamp} | {self.symbol} | {self.event_type} | "
            f"{price} | {size}"
        )

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
            "mode": self.mode,
        }

class AlpacaStreamingService:
    """Administro una ejecución Alpaca independiente de Streamlit."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        run_id: str,
        symbols: list[str],
        feed: str = "iex",
        mode: str = "LIVE",
        replay_speed: float = 1.0,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._run_id = run_id
        self._symbols = symbols
        self._feed = feed
        self._mode = mode
        self._replay_speed = replay_speed
        self._events: deque[AlpacaEvent] = deque(maxlen=VISIBLE_EVENT_LIMIT)
        self._logs: deque[str] = deque(maxlen=LOG_LIMIT)
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._stream_thread: threading.Thread | None = None
        self._supervisor_thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._websocket: ClientConnection | None = None
        self._status = "detenido"
        self._error: str | None = None
        self._end_reason: str | None = None
        self._started_at: str | None = None
        self._ended_at: str | None = None
        self._last_heartbeat_at: str | None = None
        self._last_heartbeat_monotonic = 0.0
        self._event_count = 0
        self._replay_position = 0
        self._historical_date: str | None = None
        self._finished = False
        self._metadata_path = DATA_DIRECTORY / f"{run_id}.json"
        self._events_path = DATA_DIRECTORY / f"{run_id}.jsonl"

    def start(self) -> bool:
        """Inicio los hilos de streaming y supervisión del RUN."""

        with self._lock:
            if self.is_active():
                return False
            self._status = (
                "descargando_historico"
                if self._mode == "REPLAY"
                else "conectando"
            )
            self._started_at = get_utc_timestamp()
            self._last_heartbeat_at = self._started_at
            self._last_heartbeat_monotonic = time.monotonic()
            self._add_log(f"{self._mode} | RUN iniciado")
            self._persist_metadata_locked()
            self._stream_thread = threading.Thread(
                target=self._run_stream,
                daemon=True,
                name=f"alpaca-stream-{self._run_id}",
            )
            self._supervisor_thread = threading.Thread(
                target=self._supervise_heartbeat,
                daemon=True,
                name=f"alpaca-supervisor-{self._run_id}",
            )
            self._stream_thread.start()
            self._supervisor_thread.start()
            self._add_log(f"{self._mode} | worker de supervisión iniciado")
            self._persist_metadata_locked()
        return True

    def heartbeat(self, run_id: str) -> bool:
        """Registro una señal válida proveniente de la sesión visual."""

        with self._lock:
            if run_id != self._run_id or not self.is_active():
                return False
            self._last_heartbeat_monotonic = time.monotonic()
            self._last_heartbeat_at = get_utc_timestamp()
            self._add_log(f"{self._mode} | heartbeat recibido")
            self._persist_metadata_locked()
        return True

    def stop(self, end_reason: str) -> None:
        """Detengo el RUN y solicito el cierre real del WebSocket."""

        with self._lock:
            if self._finished:
                return
            self._status = "finalizando"
            self._end_reason = end_reason
            self._stop_event.set()
            self._add_log(f"{self._mode} | detención solicitada: {end_reason}")
            loop = self._loop
            task = self._task
            websocket = self._websocket
            self._persist_metadata_locked()
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
            self._finish("detenido", end_reason)

    def is_active(self) -> bool:
        """Indico si el RUN sigue aceptando heartbeats."""

        return not self._finished and self._status in {
            "conectando",
            "conectado",
            "descargando_historico",
            "reproduciendo",
            "finalizando",
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
                "mode": self._mode,
                "replay_speed": self._replay_speed,
                "status": self._status,
                "error": self._error,
                "event_count": self._event_count,
                "replay_position": self._replay_position,
                "historical_date": self._historical_date,
                "logs": list(self._logs),
                "started_at": self._started_at,
                "ended_at": self._ended_at,
                "last_heartbeat": self._last_heartbeat_at,
                "end_reason": self._end_reason,
                "is_active": self.is_active(),
            }

    def get_recent_events(self, limit: int) -> list[AlpacaEvent]:
        """Devuelvo únicamente los últimos eventos mantenidos en memoria."""

        with self._lock:
            return list(self._events)[-limit:]

    def get_recent_logs(self, limit: int) -> list[str]:
        """Devuelvo los mensajes recientes del ciclo de ejecución."""

        with self._lock:
            return list(self._logs)[-limit:]

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
                    "error",
                    "connection_error",
                    self._safe_error(error),
                )
        finally:
            self._clear_async_resources()
            if self._stop_event.is_set():
                self._finish("detenido", self._end_reason or "manual_stop")

    def _run_replay(self) -> None:
        """Descargo y reproduzco eventos históricos de forma progresiva."""

        self._add_log("REPLAY | buscando histórico local o remoto")
        historical_path, historical_date = get_historical_dataset(
            self._api_key,
            self._api_secret,
            self._symbols,
            self._feed,
            progress_callback=self._add_log,
        )
        with self._lock:
            self._historical_date = historical_date
            self._status = "reproduciendo"
            self._add_log(
                f"REPLAY | histórico listo: {historical_date} | "
                f"archivo={historical_path.name}"
            )
            self._persist_metadata_locked()
        with historical_path.open(encoding="utf-8") as historical_file:
            for position, line in enumerate(historical_file, start=1):
                if self._stop_event.is_set():
                    return
                record = json.loads(line)
                event = event_from_record(record, mode="REPLAY")
                self._persist_event(event, replay_position=position)
                if position == 1 or position % 100 == 0:
                    self._add_log(
                        f"REPLAY | eventos emitidos={position}"
                    )
                if self._replay_speed > 0 and self._stop_event.wait(
                    self._replay_speed
                ):
                    return
        self._finish("completado", "replay_completed")

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
                    }
                )
            )
            self._set_status("conectado", None)
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

    def _supervise_heartbeat(self) -> None:
        """Detengo el RUN cuando la página deja de informar actividad."""

        while not self._stop_event.wait(SUPERVISOR_INTERVAL_SECONDS):
            with self._lock:
                elapsed = time.monotonic() - self._last_heartbeat_monotonic
            if elapsed > HEARTBEAT_TIMEOUT_SECONDS:
                self.stop("heartbeat_timeout")
                return

    def _handle_message(self, message: str | bytes) -> None:
        """Transformo y persisto cada operación recibida de Alpaca."""

        for payload in self._decode_message(message):
            event_type = payload.get("T")
            if event_type == "error":
                self._finish(
                    "error",
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
                price=self._number(payload.get("p", payload.get("ap"))),
                size=self._number(payload.get("s", payload.get("as"))),
            )
            self._persist_event(event)

    def _persist_event(
        self,
        event: AlpacaEvent,
        replay_position: int | None = None,
    ) -> None:
        """Agrego un evento al JSONL sin sobrescribir el historial."""

        with self._lock:
            ensure_data_directory()
            record = event.to_record(self._run_id)
            with self._events_path.open("a", encoding="utf-8") as event_file:
                event_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                event_file.flush()
                os.fsync(event_file.fileno())
            self._events.append(event)
            self._event_count += 1
            if replay_position is not None:
                self._replay_position = replay_position
            self._persist_metadata_locked()

    def _set_status(self, status: str, error: str | None) -> None:
        """Actualizo y persisto el estado transitorio de la conexión."""

        with self._lock:
            if self._finished:
                return
            self._status = status
            self._error = error
            self._persist_metadata_locked()

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
            self._persist_metadata_locked()
        release_active_service(self)

    def _persist_metadata_locked(self) -> None:
        """Escribo el estado completo del RUN de forma atómica."""

        ensure_data_directory()
        metadata = self.get_snapshot()
        metadata["events_file"] = self._events_path.name
        write_json(self._metadata_path, metadata)

    def _add_log(self, message: str) -> None:
        """Agrego un mensaje operativo al log visible del RUN."""

        timestamp = get_utc_timestamp()
        with self._lock:
            self._logs.append(f"{timestamp} | {message}")

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
        for thread in (self._stream_thread, self._supervisor_thread):
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
        """Formateo el horario del evento para la consola."""

        if not value:
            return datetime.now().astimezone().strftime("%H:%M:%S")
        return str(value)[11:19]

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

def ensure_data_directory() -> None:
    """Creo el directorio de registros cuando todavía no existe."""

    DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

def get_utc_timestamp() -> str:
    """Genero una marca temporal UTC reproducible para cada registro."""

    return datetime.now(timezone.utc).isoformat()

def write_json(path: Path, content: dict[str, Any]) -> None:
    """Reemplazo la metadata de forma atómica y consistente."""

    temporary_path = path.with_suffix(".tmp")
    with temporary_path.open("w", encoding="utf-8") as metadata_file:
        json.dump(content, metadata_file, ensure_ascii=False, indent=2)
        metadata_file.flush()
        os.fsync(metadata_file.fileno())
    temporary_path.replace(path)

def read_json(path: Path) -> dict[str, Any] | None:
    """Leo la metadata de un RUN cuando el archivo es válido."""

    try:
        with path.open(encoding="utf-8") as metadata_file:
            content = json.load(metadata_file)
    except (OSError, json.JSONDecodeError):
        return None
    return content if isinstance(content, dict) else None

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

def get_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[Path, str]:
    """Obtengo un histórico válido o reutilizo una copia local completa."""

    ensure_historical_directory()
    last_error: Exception | None = None
    current_date = datetime.now(NEW_YORK_TIMEZONE).date() - timedelta(days=1)
    checked_dates = 0
    while checked_dates < 10:
        if current_date.weekday() < 5:
            date_text = current_date.isoformat()
            historical_path = historical_file_path(date_text, symbols, feed)
            metadata = read_json(historical_metadata_path(historical_path))
            if (
                metadata is not None
                and metadata.get("complete") is True
                and int(metadata.get("event_count", 0)) > 0
                and historical_path.exists()
            ):
                if progress_callback is not None:
                    progress_callback(
                        f"REPLAY | caché local reutilizado: {date_text} | "
                        f"eventos={metadata['event_count']}"
                    )
                return historical_path, date_text
            try:
                event_count = download_historical_dataset(
                    api_key,
                    api_secret,
                    symbols,
                    feed,
                    date_text,
                    historical_path,
                    progress_callback,
                )
                if event_count > 0:
                    return historical_path, date_text
            except (OSError, RuntimeError, urllib.error.URLError) as error:
                last_error = error
            checked_dates += 1
        current_date -= timedelta(days=1)
    if last_error is not None:
        raise RuntimeError(
            f"No se pudo obtener un histórico de Alpaca: {last_error}"
        ) from last_error
    raise RuntimeError(
        "Alpaca no devolvió operaciones históricas disponibles."
    )

def download_historical_dataset(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str,
    date_text: str,
    historical_path: Path,
    progress_callback: Callable[[str], None] | None = None,
) -> int:
    """Descargo páginas de operaciones y guardo una copia JSONL completa."""

    temporary_path = historical_path.with_suffix(".tmp")
    query_symbols = ",".join(symbols)
    next_page_token: str | None = None
    event_count = 0
    page_number = 0
    ensure_historical_directory()
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with temporary_path.open("w", encoding="utf-8") as historical_file:
            while True:
                page_number += 1
                query = {
                    "symbols": query_symbols,
                    "start": f"{date_text}T00:00:00Z",
                    "end": f"{date_text}T23:59:59Z",
                    "feed": feed,
                    "limit": "10000",
                }
                if next_page_token:
                    query["page_token"] = next_page_token
                url = f"{HISTORICAL_API_URL}?{urllib.parse.urlencode(query)}"
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
                        payload = json.load(response)
                except urllib.error.HTTPError as error:
                    if error.code in {401, 403}:
                        raise RuntimeError(
                            "Alpaca rechazó el acceso al histórico."
                        ) from error
                    raise RuntimeError(
                        f"Alpaca devolvió HTTP {error.code}."
                    ) from error
                trades_by_symbol = payload.get("trades", {})
                for symbol, trades in trades_by_symbol.items():
                    for trade in trades:
                        record = {
                            "symbol": symbol,
                            "event_type": "TRADE",
                            "timestamp": trade.get("t"),
                            "price": trade.get("p"),
                            "size": trade.get("s"),
                            "mode": "REPLAY",
                            "source": "alpaca_historical",
                            "raw": trade,
                        }
                        historical_file.write(
                            json.dumps(record, ensure_ascii=False) + "\n"
                        )
                        event_count += 1
                if progress_callback is not None:
                    progress_callback(
                        f"REPLAY | descarga página={page_number} | "
                        f"eventos acumulados={event_count}"
                    )
                next_page_token = payload.get("next_page_token")
                if not next_page_token:
                    break
            historical_file.flush()
            os.fsync(historical_file.fileno())
        if event_count == 0:
            temporary_path.unlink(missing_ok=True)
            return 0
        temporary_path.replace(historical_path)
        write_json(
            historical_metadata_path(historical_path),
            {
                "complete": True,
                "event_count": event_count,
                "feed": feed,
                "symbols": symbols,
                "date": date_text,
                "events_file": historical_path.name,
            },
        )
        return event_count
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

def ensure_historical_directory() -> None:
    """Creo el directorio separado para copias históricas originales."""

    HISTORICAL_DIRECTORY.mkdir(parents=True, exist_ok=True)

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

def historical_metadata_path(historical_path: Path) -> Path:
    """Obtengo la ruta de metadata asociada a un histórico."""

    return historical_path.with_suffix(".json")

def get_latest_metadata() -> dict[str, Any] | None:
    """Obtengo el RUN más reciente sin cargar sus eventos completos."""

    if not DATA_DIRECTORY.exists():
        return None
    metadata_paths = list(DATA_DIRECTORY.glob("*.json"))
    if not metadata_paths:
        return None
    latest_path = max(metadata_paths, key=lambda path: path.stat().st_mtime)
    return read_json(latest_path)

def mark_interrupted_run() -> dict[str, Any] | None:
    """Reconozco un RUN activo que pertenecía a otro proceso Python."""

    metadata = get_latest_metadata()
    if metadata is None or not metadata.get("is_active"):
        return metadata
    metadata["status"] = "incompleto"
    metadata["is_active"] = False
    metadata["end_reason"] = "process_restart"
    metadata["ended_at"] = get_utc_timestamp()
    run_id = metadata.get("run_id")
    if isinstance(run_id, str):
        write_json(DATA_DIRECTORY / f"{run_id}.json", metadata)
    return metadata

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
    )

def to_number(value: Any) -> float | None:
    """Convierto números persistidos a un valor seguro para la interfaz."""

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

def read_recent_events(run_id: str, limit: int) -> list[AlpacaEvent]:
    """Leo solamente las últimas líneas JSONL requeridas por la consola."""

    events_path = DATA_DIRECTORY / f"{run_id}.jsonl"
    if not events_path.exists():
        return []
    try:
        with events_path.open(encoding="utf-8") as events_file:
            lines = deque(events_file, maxlen=limit)
    except OSError:
        return []
    events: list[AlpacaEvent] = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            events.append(event_from_record(record))
    return events

def start_run(
    api_key: str,
    api_secret: str,
    symbols: list[str],
    feed: str = "iex",
    replay_speed: float = 1.0,
) -> AlpacaStreamingService:
    """Creo un único RUN y comienzo su ciclo de vida."""

    global _ACTIVE_SERVICE
    with _MANAGER_LOCK:
        if _ACTIVE_SERVICE is not None and _ACTIVE_SERVICE.is_active():
            return _ACTIVE_SERVICE
        selected_mode = get_market_mode(api_key, api_secret)
        run_id = uuid.uuid4().hex
        _ACTIVE_SERVICE = AlpacaStreamingService(
            api_key,
            api_secret,
            run_id,
            symbols,
            feed,
            selected_mode,
            replay_speed,
        )
        _ACTIVE_SERVICE.start()
        return _ACTIVE_SERVICE

def stop_active_run(end_reason: str) -> None:
    """Detengo el único RUN LIVE administrado por el proyecto."""

    service = get_active_service()
    if service is not None:
        service.stop(end_reason)

def release_active_service(service: AlpacaStreamingService) -> None:
    """Libero el RUN terminado para permitir una nueva ejecución."""

    global _ACTIVE_SERVICE
    with _MANAGER_LOCK:
        if _ACTIVE_SERVICE is service:
            _ACTIVE_SERVICE = None

def get_active_service() -> AlpacaStreamingService | None:
    """Devuelvo el RUN LIVE compartido por las sesiones de Streamlit."""

    with _MANAGER_LOCK:
        return _ACTIVE_SERVICE

def get_run_snapshot() -> dict[str, Any]:
    """Obtengo el estado live o la última ejecución persistida."""

    service = get_active_service()
    if service is not None:
        return service.get_snapshot()
    metadata = get_latest_metadata()
    if metadata is not None:
        metadata.setdefault("mode", "LIVE")
        metadata.setdefault("replay_speed", 1.0)
        metadata.setdefault("replay_position", 0)
        metadata.setdefault("historical_date", None)
        metadata.setdefault("logs", [])
        return metadata
    return {
        "run_id": None,
        "symbols": [],
        "mode": "LIVE",
        "replay_speed": 1.0,
        "status": "detenido",
        "error": None,
        "event_count": 0,
        "replay_position": 0,
        "historical_date": None,
        "logs": [],
        "started_at": None,
        "ended_at": None,
        "last_heartbeat": None,
        "end_reason": None,
        "is_active": False,
    }

def register_heartbeat(run_id: str) -> bool:
    """Valido y registro el heartbeat de una sesión visual activa."""

    service = get_active_service()
    return service.heartbeat(run_id) if service is not None else False

def get_recent_events(run_id: str | None, limit: int) -> list[AlpacaEvent]:
    """Devuelvo eventos activos o persistidos sin cargar el log completo."""

    if run_id is None:
        return []
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        return service.get_recent_events(limit)
    return read_recent_events(run_id, limit)

def get_recent_logs(run_id: str | None, limit: int) -> list[str]:
    """Devuelvo logs activos o persistidos sin duplicar mensajes."""

    if run_id is None:
        return []
    service = get_active_service()
    if service is not None and service.get_snapshot()["run_id"] == run_id:
        return service.get_recent_logs(limit)
    metadata = get_latest_metadata()
    if metadata is None or metadata.get("run_id") != run_id:
        return []
    return list(metadata.get("logs", []))[-limit:]

def shutdown_active_run() -> None:
    """Cierro el RUN administrado cuando finaliza el proceso Python."""

    stop_active_run("process_shutdown")

_MANAGER_LOCK = threading.RLock()
_ACTIVE_SERVICE: AlpacaStreamingService | None = None
mark_interrupted_run()
atexit.register(shutdown_active_run)
