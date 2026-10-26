"""Cliente de streaming WebSocket para Alpaca Market Data."""

import asyncio
import certifi
import json
import queue
import ssl
import threading

from datetime import datetime
from typing import Any
from websockets.asyncio.client import ClientConnection, connect

ALPACA_API_KEY = "PKJQMFWVYH6PEXUKK34QAABOUW"
ALPACA_SECRET_KEY = "5xBskyt9ofjunrzuCa77RDwjP5n5fGCDDv6XrfxqzAyk"

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
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.event_type = event_type
        self.price = price
        self.size = size

    def to_display_line(self) -> str:
        """Convierto el evento a una línea segura para mostrarlo."""

        price = "-" if self.price is None else f"{self.price:.4f}"
        size = "-" if self.size is None else f"{self.size:g}"
        return (
            f"{self.timestamp} | {self.symbol} | {self.event_type} | "
            f"{price} | {size}"
        )

class AlpacaStreamingService:
    """Administro una conexión Alpaca sin bloquear el hilo de Streamlit."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        feed: str = "iex",
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._feed = feed
        self._events: queue.Queue[AlpacaEvent] = queue.Queue(maxsize=200)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._status = "detenido"
        self._error: str | None = None

    def get_status(self) -> str:
        """Devuelvo el estado actual de la conexión."""

        with self._lock:
            return self._status

    def get_error(self) -> str | None:
        """Devuelvo el último error sin exponer credenciales."""

        with self._lock:
            return self._error

    def is_running(self) -> bool:
        """Indico si el hilo de conexión sigue activo."""

        return self._thread is not None and self._thread.is_alive()

    def start(self, symbols: list[str]) -> bool:
        """Inicio una única conexión para los símbolos indicados."""

        if self.is_running() or not symbols:
            return False

        self._stop_event.clear()
        self._set_state("conectando", None)
        self._thread = threading.Thread(
            target=self._run_thread,
            args=(symbols,),
            daemon=True,
            name="alpaca-streaming",
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        """Solicito el cierre de la conexión sin bloquear Streamlit."""

        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)
        self._set_state("detenido", None)

    def drain_events(self) -> list[AlpacaEvent]:
        """Retiro los eventos recibidos y conservo una cantidad acotada."""

        events: list[AlpacaEvent] = []
        while True:
            try:
                events.append(self._events.get_nowait())
            except queue.Empty:
                return events

    def _run_thread(self, symbols: list[str]) -> None:
        try:
            asyncio.run(self._receive_events(symbols))
        except Exception as error:  # noqa: BLE001
            if not self._stop_event.is_set():
                self._set_state("error", self._safe_error(error))
        finally:
            if self._stop_event.is_set():
                self._set_state("detenido", None)

    async def _receive_events(self, symbols: list[str]) -> None:
        uri = f"wss://stream.data.alpaca.markets/v2/{self._feed}"
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with connect(
            uri,
            ping_interval=20,
            ping_timeout=20,
            ssl=ssl_context,
        ) as websocket:
            await self._authenticate(websocket)
            await websocket.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "trades": symbols,
                    }
                )
            )
            self._set_state("conectado", None)

            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=1,
                    )
                except asyncio.TimeoutError:
                    continue
                self._handle_message(message)

    async def _authenticate(self, websocket: ClientConnection) -> None:
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
            if any(item.get("T") == "error" for item in payload):
                raise RuntimeError("Alpaca rechazó la autenticación")

    def _handle_message(self, message: str | bytes) -> None:
        for payload in self._decode_message(message):
            event_type = payload.get("T")
            if event_type == "error":
                self._set_state(
                    "error",
                    str(payload.get("msg", "Error de Alpaca")),
                )
                continue
            if event_type not in {"t", "q"}:
                continue

            event = AlpacaEvent(
                timestamp=self._format_timestamp(payload.get("t")),
                symbol=str(payload.get("S", "-")),
                event_type="TRADE" if event_type == "t" else "QUOTE",
                price=self._number(payload.get("p", payload.get("ap"))),
                size=self._number(payload.get("s", payload.get("as"))),
            )
            try:
                self._events.put_nowait(event)
            except queue.Full:
                try:
                    self._events.get_nowait()
                    self._events.put_nowait(event)
                except queue.Empty:
                    pass

    def _decode_message(self, message: str | bytes) -> list[dict[str, Any]]:
        decoded = json.loads(message)
        return decoded if isinstance(decoded, list) else [decoded]

    def _format_timestamp(self, value: Any) -> str:
        if not value:
            return datetime.now().astimezone().strftime("%H:%M:%S")
        return str(value)[11:19]

    def _number(self, value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _safe_error(self, error: Exception) -> str:
        message = str(error)
        return message.replace("key=", "key=[oculta]").replace(
            "secret=", "secret=[oculta]"
        )

    def _set_state(self, status: str, error: str | None) -> None:
        with self._lock:
            self._status = status
            self._error = error

def create_service(
    api_key: str,
    api_secret: str,
    feed: str = "iex",
) -> AlpacaStreamingService:
    """Creo el servicio de Alpaca con credenciales de la interfaz."""

    return AlpacaStreamingService(api_key, api_secret, feed)
