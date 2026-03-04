"""
Streaming producer.

Puts market-data ticks onto the chosen transport:
  - In-memory ``queue.Queue`` (default, no external dependencies)
  - Apache Kafka           (when ``config.USE_KAFKA = True``)

Run this module directly to watch the producer in action:
    python producer.py
"""

import json
import logging
import queue
import threading
import time
from typing import Optional

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_MARKET,
    TICK_INTERVAL,
    USE_KAFKA,
)
from data_generator import MarketDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# In-memory producer (works without Kafka)
# ---------------------------------------------------------------------------

class InMemoryProducer:
    """
    Streams ticks into a shared ``queue.Queue``.

    The producer runs in a background daemon thread so the calling process
    can continue without blocking.

    Parameters
    ----------
    data_queue : queue.Queue
        Shared queue that consumers will read from.
    symbols : list[str], optional
        Symbols to generate data for.
    tick_interval : float, optional
        Seconds between full rounds of ticks.
    """

    def __init__(
        self,
        data_queue: queue.Queue,
        symbols: Optional[list[str]] = None,
        tick_interval: float = TICK_INTERVAL,
    ) -> None:
        self.queue = data_queue
        self.generator = MarketDataGenerator(symbols=symbols, tick_interval=tick_interval)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    def start(self) -> None:
        """Launch the producer in a daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="producer")
        self._thread.start()
        logger.info("InMemoryProducer started (symbols: %s)", self.generator.symbols)

    def stop(self) -> None:
        """Signal the producer thread to stop."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("InMemoryProducer stopped.")

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------
    def _run(self) -> None:
        for tick in self.generator.stream():
            if self._stop_event.is_set():
                break
            try:
                self.queue.put_nowait(tick)
                logger.debug("Queued tick: %s @ %.4f", tick["symbol"], tick["price"])
            except queue.Full:
                logger.warning("Queue full — dropping tick for %s", tick["symbol"])


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

class KafkaStreamProducer:
    """
    Publishes ticks to a Kafka topic.

    Requires ``kafka-python`` and a running Kafka broker
    (see ``docker-compose.yml``).

    Parameters
    ----------
    bootstrap_servers : str
        Kafka broker address (e.g. ``"localhost:9092"``).
    topic : str
        Kafka topic name.
    symbols : list[str], optional
        Symbols to stream.
    tick_interval : float, optional
        Seconds between rounds.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC_MARKET,
        symbols: Optional[list[str]] = None,
        tick_interval: float = TICK_INTERVAL,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.generator = MarketDataGenerator(symbols=symbols, tick_interval=tick_interval)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._producer = None  # lazy init

    # ------------------------------------------------------------------
    def start(self) -> None:
        self._init_producer()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="kafka-producer")
        self._thread.start()
        logger.info("KafkaStreamProducer started → topic '%s'", self.topic)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._producer:
            self._producer.flush()
            self._producer.close()
        logger.info("KafkaStreamProducer stopped.")

    # ------------------------------------------------------------------
    def _init_producer(self) -> None:
        try:
            from kafka import KafkaProducer  # type: ignore
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Connected to Kafka at %s", self.bootstrap_servers)
        except ImportError:
            raise RuntimeError(
                "kafka-python not installed.  Run: pip install kafka-python"
            )
        except Exception as exc:
            raise RuntimeError(
                f"Could not connect to Kafka at {self.bootstrap_servers}: {exc}"
            ) from exc

    def _run(self) -> None:
        for tick in self.generator.stream():
            if self._stop_event.is_set():
                break
            future = self._producer.send(self.topic, value=tick, key=tick["symbol"].encode())
            logger.debug("Published: %s @ %.4f", tick["symbol"], tick["price"])


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

def create_producer(data_queue: Optional[queue.Queue] = None, **kwargs):
    """
    Return the appropriate producer based on ``config.USE_KAFKA``.

    Parameters
    ----------
    data_queue : queue.Queue, optional
        Required for in-memory mode.
    **kwargs
        Forwarded to the producer constructor.
    """
    if USE_KAFKA:
        logger.info("Using KafkaStreamProducer")
        return KafkaStreamProducer(**kwargs)
    if data_queue is None:
        raise ValueError("data_queue is required for InMemoryProducer")
    logger.info("Using InMemoryProducer")
    return InMemoryProducer(data_queue, **kwargs)


# ---------------------------------------------------------------------------
# Quick standalone demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import signal

    shared_queue: queue.Queue = queue.Queue(maxsize=500)
    producer = InMemoryProducer(shared_queue)
    producer.start()

    def _shutdown(sig, frame):
        print("\nStopping...")
        producer.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)

    print("Producer running. Press Ctrl+C to stop.\n")
    tick_num = 0
    while producer.is_running:
        try:
            tick = shared_queue.get(timeout=2)
            tick_num += 1
            print(
                f"[{tick_num:>4}] {tick['symbol']:<10} "
                f"price={tick['price']:>10.4f}  "
                f"vol={tick['volume']:>6}  "
                f"change={tick['change_pct']:>+7.4f}%"
            )
        except queue.Empty:
            continue
