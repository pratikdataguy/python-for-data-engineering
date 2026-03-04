"""
Streaming consumer with real-time analytics.

Reads ticks from either an in-memory queue or Kafka, applies:
  - Rolling statistics (VWAP, moving averages, volatility)
  - Price-change alerts
  - Optional CSV/JSONL export

Run standalone:
    python consumer.py
"""

import csv
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

import pandas as pd

from config import (
    ALERT_THRESHOLD_HIGH,
    ALERT_THRESHOLD_LOW,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_MARKET,
    MAX_HISTORY,
    USE_KAFKA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  %(message)s",
)
logger = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# Analytics engine
# ---------------------------------------------------------------------------

class StreamAnalytics:
    """
    Maintains per-symbol rolling statistics over a sliding window of ticks.

    Attributes updated on each ``process`` call
    -------------------------------------------
    prices         : deque of recent prices
    volumes        : deque of recent volumes
    sma_20         : 20-tick simple moving average
    sma_50         : 50-tick simple moving average
    vwap           : volume-weighted average price (rolling window)
    volatility     : rolling standard deviation of log-returns
    cumulative_vol : total volume seen
    tick_count     : total ticks processed
    """

    def __init__(self, window: int = MAX_HISTORY) -> None:
        self.window = window
        self.prices: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self.volumes: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self.cumulative_volume: dict[str, int] = defaultdict(int)
        self.tick_counts: dict[str, int] = defaultdict(int)
        self.alerts: list[dict] = []

    def process(self, tick: dict) -> dict:
        """
        Ingest one tick and return an enriched record with analytics attached.
        """
        symbol = tick["symbol"]
        price = tick["price"]
        volume = tick["volume"]

        self.prices[symbol].append(price)
        self.volumes[symbol].append(volume)
        self.cumulative_volume[symbol] += volume
        self.tick_counts[symbol] += 1

        enriched = dict(tick)
        enriched.update(self._compute_stats(symbol))

        # Alert generation
        alert = self._check_alert(enriched)
        if alert:
            self.alerts.append(alert)
            enriched["alert"] = alert["type"]
        else:
            enriched["alert"] = None

        return enriched

    def _compute_stats(self, symbol: str) -> dict:
        prices = list(self.prices[symbol])
        volumes = list(self.volumes[symbol])

        sma_20 = self._sma(prices, 20)
        sma_50 = self._sma(prices, 50)
        vwap = self._vwap(prices, volumes)
        volatility = self._volatility(prices)

        return {
            "sma_20": sma_20,
            "sma_50": sma_50,
            "vwap": vwap,
            "volatility": volatility,
            "cumulative_volume": self.cumulative_volume[symbol],
            "tick_count": self.tick_counts[symbol],
        }

    @staticmethod
    def _sma(prices: list, period: int) -> Optional[float]:
        if len(prices) < period:
            return None
        return round(sum(prices[-period:]) / period, 4)

    @staticmethod
    def _vwap(prices: list, volumes: list) -> Optional[float]:
        if not prices:
            return None
        pv = sum(p * v for p, v in zip(prices, volumes))
        total_vol = sum(volumes)
        return round(pv / total_vol, 4) if total_vol else None

    @staticmethod
    def _volatility(prices: list, period: int = 20) -> Optional[float]:
        if len(prices) < 2:
            return None
        import math
        subset = prices[-period:]
        log_returns = [
            math.log(subset[i] / subset[i - 1])
            for i in range(1, len(subset))
        ]
        if not log_returns:
            return None
        mean = sum(log_returns) / len(log_returns)
        variance = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
        return round(math.sqrt(variance) * 100, 6)  # as percentage

    def _check_alert(self, enriched: dict) -> Optional[dict]:
        change = enriched.get("change_pct", 0)
        if change >= ALERT_THRESHOLD_HIGH:
            atype = "PRICE_SURGE"
        elif change <= ALERT_THRESHOLD_LOW:
            atype = "PRICE_DROP"
        else:
            return None
        return {
            "type": atype,
            "symbol": enriched["symbol"],
            "price": enriched["price"],
            "change_pct": change,
            "timestamp": enriched["timestamp"],
        }

    def summary_dataframe(self) -> pd.DataFrame:
        """Return a summary DataFrame with the latest tick for every symbol."""
        rows = []
        for symbol in self.prices:
            prices = list(self.prices[symbol])
            if not prices:
                continue
            rows.append({
                "symbol": symbol,
                "latest_price": prices[-1],
                "sma_20": self._sma(prices, 20),
                "sma_50": self._sma(prices, 50),
                "tick_count": self.tick_counts[symbol],
                "total_volume": self.cumulative_volume[symbol],
            })
        return pd.DataFrame(rows).set_index("symbol") if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# In-memory consumer
# ---------------------------------------------------------------------------

class InMemoryConsumer:
    """
    Reads ticks from a shared ``queue.Queue`` and applies analytics.

    Processed ticks are pushed into ``processed_queue`` so the Streamlit
    dashboard (or any downstream component) can read them.

    Parameters
    ----------
    input_queue : queue.Queue
        Ticks from the producer.
    processed_queue : queue.Queue
        Enriched ticks for the dashboard / downstream.
    export_path : str, optional
        If provided, enriched ticks are appended to this JSONL file.
    """

    def __init__(
        self,
        input_queue: queue.Queue,
        processed_queue: queue.Queue,
        export_path: Optional[str] = None,
    ) -> None:
        self.input_queue = input_queue
        self.processed_queue = processed_queue
        self.analytics = StreamAnalytics()
        self.export_path = export_path
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._processed_count = 0

    # ------------------------------------------------------------------
    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="consumer")
        self._thread.start()
        logger.info("InMemoryConsumer started.")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("InMemoryConsumer stopped. Processed %d ticks.", self._processed_count)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------
    def _run(self) -> None:
        export_file = None
        if self.export_path:
            export_file = open(self.export_path, "a")

        try:
            while not self._stop_event.is_set():
                try:
                    tick = self.input_queue.get(timeout=1)
                except queue.Empty:
                    continue

                enriched = self.analytics.process(tick)
                self._processed_count += 1

                # Push to processed queue (drop if full to avoid back-pressure)
                try:
                    self.processed_queue.put_nowait(enriched)
                except queue.Full:
                    pass

                if export_file:
                    export_file.write(json.dumps(enriched) + "\n")
                    export_file.flush()

                logger.debug(
                    "Processed %s  price=%.4f  sma_20=%s  vwap=%s",
                    enriched["symbol"],
                    enriched["price"],
                    enriched.get("sma_20"),
                    enriched.get("vwap"),
                )
        finally:
            if export_file:
                export_file.close()


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------

class KafkaStreamConsumer:
    """
    Reads ticks from Kafka and publishes enriched records to ``processed_queue``.
    """

    def __init__(
        self,
        processed_queue: queue.Queue,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC_MARKET,
        group_id: str = KAFKA_GROUP_ID,
    ) -> None:
        self.processed_queue = processed_queue
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.analytics = StreamAnalytics()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="kafka-consumer")
        self._thread.start()
        logger.info("KafkaStreamConsumer started ← topic '%s'", self.topic)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        from kafka import KafkaConsumer  # type: ignore

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        for message in consumer:
            if self._stop_event.is_set():
                break
            tick = message.value
            enriched = self.analytics.process(tick)
            try:
                self.processed_queue.put_nowait(enriched)
            except queue.Full:
                pass
        consumer.close()


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

def create_consumer(
    processed_queue: queue.Queue,
    input_queue: Optional[queue.Queue] = None,
    **kwargs,
):
    if USE_KAFKA:
        return KafkaStreamConsumer(processed_queue, **kwargs)
    if input_queue is None:
        raise ValueError("input_queue is required for InMemoryConsumer")
    return InMemoryConsumer(input_queue, processed_queue, **kwargs)


# ---------------------------------------------------------------------------
# Standalone demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import signal
    from producer import InMemoryProducer

    raw_q: queue.Queue = queue.Queue(maxsize=1000)
    processed_q: queue.Queue = queue.Queue(maxsize=1000)

    producer = InMemoryProducer(raw_q)
    consumer = InMemoryConsumer(raw_q, processed_q, export_path="stream_output.jsonl")

    producer.start()
    consumer.start()

    def _shutdown(sig, frame):
        print("\nStopping...")
        producer.stop()
        consumer.stop()
        print("\nSummary:\n", consumer.analytics.summary_dataframe())
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)

    print("Consumer running. Press Ctrl+C to stop.\n")
    while True:
        try:
            enriched = processed_q.get(timeout=2)
            print(
                f"{enriched['symbol']:<10} "
                f"price={enriched['price']:>10.4f}  "
                f"sma20={str(enriched.get('sma_20') or 'N/A'):>10}  "
                f"vwap={str(enriched.get('vwap') or 'N/A'):>10}  "
                f"vol%={str(enriched.get('volatility') or 'N/A'):>10}  "
                + (f"⚠ {enriched['alert']}" if enriched.get("alert") else "")
            )
        except queue.Empty:
            continue
