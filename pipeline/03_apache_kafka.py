import atexit
import json
import os
import threading
import time

from app import functions
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from pathlib import Path
from typing import Any

_KAFKA_LOCK = threading.RLock()
_KAFKA_PRODUCER: KafkaProducer | None = None
_RUN_CONNECTIONS: set[str] = set()
_RUN_STATS: dict[str, dict[str, Any]] = {}
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_RETRY_BACKOFF_SECONDS = 2
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iapd-market-events")

def close_producer() -> None:
    """Cierro el productor Kafka compartido cuando finaliza el proceso."""

    global _KAFKA_PRODUCER
    with _KAFKA_LOCK:
        if _KAFKA_PRODUCER is not None:
            _KAFKA_PRODUCER.flush(timeout=10)
            _KAFKA_PRODUCER.close(timeout=10)
            _KAFKA_PRODUCER = None

def get_log_path(run_id: str) -> Path:
    """Obtengo la ruta del log Kafka correspondiente al RUN."""

    return DATA_DIRECTORY / "runs" / f"{run_id}_03_apache_kafka_log.txt"

def get_producer() -> KafkaProducer:
    """Obtengo el productor Kafka único del proceso Streamlit."""

    global _KAFKA_PRODUCER
    with _KAFKA_LOCK:
        if _KAFKA_PRODUCER is None:
            _KAFKA_PRODUCER = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                acks="all",
                retries=5,
                request_timeout_ms=10000,
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False,
                ).encode("utf-8"),
            )
        return _KAFKA_PRODUCER

def reset_producer() -> None:
    """Descarto el productor Kafka cuando perdió la conexión."""

    global _KAFKA_PRODUCER
    with _KAFKA_LOCK:
        producer = _KAFKA_PRODUCER
        _KAFKA_PRODUCER = None
    if producer is not None:
        try:
            producer.close(timeout=1)
        except Exception:
            pass

def ensure_topic() -> None:
    """Creo el tópico real antes de iniciar el consumidor Structured Streaming."""

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        request_timeout_ms=10000,
    )
    try:
        admin.create_topics(
            [NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)],
            validate_only=False,
        )
    except Exception as error:
        if "TopicAlreadyExists" not in str(error):
            raise
    finally:
        admin.close()

def initialize_run(run_id: str) -> Path:
    """Preparo el estado Kafka sin crear un TXT vacío para el RUN."""

    path = get_log_path(run_id)
    with _KAFKA_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        _RUN_CONNECTIONS.discard(run_id)
        _RUN_STATS.pop(run_id, None)
    return path

def publish_event(
    run_id: str,
    event: dict[str, Any],
    stop_event: threading.Event | None = None,
) -> None:
    """Publico un evento original en Kafka y espero su confirmación."""

    publish_events(run_id, [event], stop_event)

def publish_events(
    run_id: str,
    events: list[dict[str, Any]],
    stop_event: threading.Event | None = None,
) -> None:
    """Publico un lote confirmado de eventos originales en Kafka."""

    if not events:
        return
    mode = str(events[0].get("mode") or "-")
    producer = None
    partitions: list[int] = []
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = get_producer()
            partitions = sorted(producer.partitions_for(KAFKA_TOPIC) or [])
            if not partitions:
                raise RuntimeError(
                    f"Kafka no informó particiones para el topic {KAFKA_TOPIC}."
                )
            break
        except KafkaError as error:
            reset_producer()
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("Publicación Kafka detenida.") from error
            functions.append_console_log(
                run_id,
                "kafka",
                f"KAFKA | run={run_id} | esperando_broker | intento={attempt} | error={error}",
            )
            if stop_event is not None:
                if stop_event.wait(KAFKA_RETRY_BACKOFF_SECONDS):
                    raise InterruptedError("Publicación Kafka detenida.") from error
            else:
                time.sleep(KAFKA_RETRY_BACKOFF_SECONDS)
    if producer is None or not partitions:
        raise RuntimeError(f"Kafka no informó particiones para el topic {KAFKA_TOPIC}.")
    if attempt > 1:
        functions.append_console_log(
            run_id,
            "kafka",
            f"KAFKA | run={run_id} | broker_restaurado | reintentos={attempt - 1}",
        )
    while True:
        try:
            futures = [
                producer.send(
                    KAFKA_TOPIC,
                    key=str(event.get("symbol") or run_id).encode("utf-8"),
                    value=event,
                )
                for event in events
            ]
            metadata = [future.get(timeout=10) for future in futures]
            break
        except KafkaError as error:
            reset_producer()
            attempt += 1
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("Publicación Kafka detenida.") from error
            functions.append_console_log(
                run_id,
                "kafka",
                f"KAFKA | run={run_id} | esperando_broker | intento={attempt} | error={error}",
            )
            if stop_event is not None:
                if stop_event.wait(KAFKA_RETRY_BACKOFF_SECONDS):
                    raise InterruptedError("Publicación Kafka detenida.") from error
            else:
                time.sleep(KAFKA_RETRY_BACKOFF_SECONDS)
            producer = None
            partitions = []
            while producer is None or not partitions:
                attempt += 1
                try:
                    producer = get_producer()
                    partitions = sorted(
                        producer.partitions_for(KAFKA_TOPIC) or []
                    )
                    if not partitions:
                        raise KafkaError(
                            f"Kafka no informó particiones para el topic {KAFKA_TOPIC}."
                        )
                except KafkaError as metadata_error:
                    reset_producer()
                    if stop_event is not None and stop_event.is_set():
                        raise InterruptedError(
                            "Publicación Kafka detenida."
                        ) from metadata_error
                    functions.append_console_log(
                        run_id,
                        "kafka",
                        f"KAFKA | run={run_id} | esperando_broker | intento={attempt} | error={metadata_error}",
                    )
                    if stop_event is not None:
                        if stop_event.wait(KAFKA_RETRY_BACKOFF_SECONDS):
                            raise InterruptedError(
                                "Publicación Kafka detenida."
                            ) from metadata_error
                    else:
                        time.sleep(KAFKA_RETRY_BACKOFF_SECONDS)
        except Exception as error:
            functions.append_console_log(
                run_id,
                "kafka",
                f"KAFKA | run={run_id} | modo={mode} | error_publicacion={error}",
            )
            raise
    partition_counts = {partition: 0 for partition in partitions}
    for record_metadata in metadata:
        partition_counts[record_metadata.partition] = partition_counts.get(
            record_metadata.partition,
            0,
        ) + 1
    trade_count = sum(
        event.get("event_type") == "TRADE" for event in events
    )
    quote_count = sum(
        event.get("event_type") == "QUOTE" for event in events
    )
    with _KAFKA_LOCK:
        stats = _RUN_STATS.setdefault(run_id, {"batch": 0, "total": 0})
        stats["batch"] += 1
        batch = int(stats["batch"])
        stats["total"] += len(events)
        total = int(stats["total"])
        should_log = (
            len(events) > 1
            or total == len(events)
            or total % 1000 < len(events)
        )
        if run_id in _RUN_CONNECTIONS:
            connection_line = None
        else:
            _RUN_CONNECTIONS.add(run_id)
            connection_line = f"KAFKA | {mode} | broker={KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC} | particiones={len(partitions)}"
    if connection_line is not None:
        functions.append_console_log(run_id, "kafka", connection_line)
    if should_log:
        distribution = ",".join(
            f"{partition}:{partition_counts.get(partition, 0)}"
            for partition in partitions
        )
        functions.append_console_log(
            run_id,
            "kafka",
            f"KAFKA | lote={batch} | eventos={len(events)} | TRADE={trade_count} | QUOTE={quote_count} | total={total} | particiones={distribution} | confirmados={len(metadata)}",
        )

atexit.register(close_producer)

