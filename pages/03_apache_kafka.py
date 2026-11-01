"""Publicación y persistencia del flujo de eventos en Apache Kafka."""

import atexit
import json
import os
import threading

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from pathlib import Path
from typing import Any

DATA_DIRECTORY = Path(__file__).resolve().parent.parent / "data"
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iapd-market-events")
_KAFKA_LOCK = threading.RLock()
_KAFKA_PRODUCER: KafkaProducer | None = None
_RUN_CONNECTIONS: set[str] = set()
_RUN_STATS: dict[str, dict[str, Any]] = {}
_CONSOLE_BUFFER_SINK: Any = None

def append_log(run_id: str, message: str) -> None:
    """Agrego un mensaje operativo al TXT persistente de Kafka."""

    path = get_log_path(run_id)
    with _KAFKA_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        sequence = 0
        if path.exists():
            with path.open(encoding="utf-8") as log_file:
                for line in log_file:
                    if line.startswith("[#"):
                        try:
                            sequence = max(
                                sequence,
                                int(line.split("]", 1)[0][2:]),
                            )
                        except (IndexError, ValueError):
                            continue
        with path.open("a", encoding="utf-8") as log_file:
            log_file.write(f"[#{sequence + 1}] {message}\n")
            log_file.flush()
            os.fsync(log_file.fileno())
        if _CONSOLE_BUFFER_SINK is not None:
            _CONSOLE_BUFFER_SINK(
                run_id,
                "kafka",
                f"[#{sequence + 1}] {message}",
            )

def configure_console_buffer(sink: Any) -> None:
    """Registro el destino en memoria de la consola Kafka."""

    global _CONSOLE_BUFFER_SINK
    _CONSOLE_BUFFER_SINK = sink

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

    return DATA_DIRECTORY / f"{run_id}_log_03_apache_kafka.txt"

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
    """Creo el TXT de Kafka sin alterar archivos de RUN anteriores."""

    path = get_log_path(run_id)
    with _KAFKA_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=False)
        _RUN_CONNECTIONS.discard(run_id)
        _RUN_STATS.pop(run_id, None)
    return path

def publish_event(run_id: str, event: dict[str, Any]) -> None:
    """Publico un evento original en Kafka y espero su confirmación."""

    publish_events(run_id, [event])

def publish_events(run_id: str, events: list[dict[str, Any]]) -> None:
    """Publico un lote confirmado de eventos originales en Kafka."""

    if not events:
        return
    mode = str(events[0].get("mode") or "-")
    try:
        producer = get_producer()
        partitions = sorted(producer.partitions_for(KAFKA_TOPIC) or [])
        if not partitions:
            raise RuntimeError(
                f"Kafka no informó particiones para el topic {KAFKA_TOPIC}."
            )
        futures = [
            producer.send(
                KAFKA_TOPIC,
                key=str(event.get("symbol") or run_id).encode("utf-8"),
                value=event,
            )
            for event in events
        ]
        metadata = [future.get(timeout=10) for future in futures]
    except Exception as error:
        append_log(
            run_id,
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
        stats = _RUN_STATS.setdefault(run_id, {"total": 0})
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
        append_log(run_id, connection_line)
    if should_log:
        distribution = ",".join(
            f"{partition}:{partition_counts.get(partition, 0)}"
            for partition in partitions
        )
        append_log(
            run_id,
            f"KAFKA | lote={len(events)} | TRADE={trade_count} | QUOTE={quote_count} | total={total} | particiones={distribution} | confirmados={len(metadata)}",
        )

atexit.register(close_producer)

