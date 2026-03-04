"""
Market data generator.

Produces realistic simulated tick data using a random-walk model with
mean-reversion, realistic bid/ask spreads, and volume profiles.
"""

import json
import random
import time
from datetime import datetime
from typing import Generator, Optional

import numpy as np

from config import INITIAL_PRICES, SYMBOLS, TICK_INTERVAL, VOLATILITY


class MarketDataGenerator:
    """
    Simulates a real-time market-data feed.

    Each call to ``generate_tick`` produces one OHLCV-style tick for the
    requested symbol (or a randomly selected one).  The price follows a
    discrete geometric Brownian-motion process with mild mean-reversion so
    prices stay in a plausible range over long runs.

    Parameters
    ----------
    symbols : list[str], optional
        Ticker symbols to track.  Defaults to ``config.SYMBOLS``.
    volatility : float, optional
        Per-tick price-change standard deviation as a fraction of price.
    tick_interval : float, optional
        Sleep duration (seconds) between ticks when using ``stream()``.
    """

    def __init__(
        self,
        symbols: Optional[list[str]] = None,
        volatility: float = VOLATILITY,
        tick_interval: float = TICK_INTERVAL,
    ) -> None:
        self.symbols = symbols or SYMBOLS
        self.volatility = volatility
        self.tick_interval = tick_interval

        # Initialise prices from config; unknown symbols get a random start
        self.prices: dict[str, float] = {
            s: INITIAL_PRICES.get(s, random.uniform(50, 500))
            for s in self.symbols
        }
        self._base_prices: dict[str, float] = dict(self.prices)  # for mean-reversion
        self._tick_count: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_tick(self, symbol: Optional[str] = None) -> dict:
        """Return a single market-data tick as a dictionary."""
        symbol = symbol or random.choice(self.symbols)
        price = self._next_price(symbol)

        spread_pct = random.uniform(0.0001, 0.0005)
        bid = round(price * (1 - spread_pct), 4)
        ask = round(price * (1 + spread_pct), 4)
        volume = self._simulate_volume(symbol)
        change_pct = round((price - self._base_prices[symbol]) / self._base_prices[symbol] * 100, 4)

        self._tick_count += 1

        return {
            "symbol": symbol,
            "price": price,
            "bid": bid,
            "ask": ask,
            "volume": volume,
            "change_pct": change_pct,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "tick_id": self._tick_count,
        }

    def stream(self, symbols: Optional[list[str]] = None) -> Generator[dict, None, None]:
        """
        Yield ticks indefinitely, cycling through all symbols each round.

        Parameters
        ----------
        symbols : list[str], optional
            Subset of symbols to stream.  Defaults to ``self.symbols``.
        """
        targets = symbols or self.symbols
        while True:
            for symbol in targets:
                yield self.generate_tick(symbol)
            time.sleep(self.tick_interval)

    def generate_batch(self, n: int = 100) -> list[dict]:
        """Return a list of ``n`` ticks spread across all symbols."""
        return [self.generate_tick() for _ in range(n)]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _next_price(self, symbol: str) -> float:
        """Advance price using GBM with mild mean-reversion."""
        current = self.prices[symbol]
        base = self._base_prices[symbol]

        # Mean-reversion force (pulls price back towards starting level)
        reversion = 0.001 * (base - current) / base

        # Random shock
        shock = np.random.normal(reversion, self.volatility)
        new_price = max(current * (1 + shock), 0.01)

        self.prices[symbol] = round(new_price, 4)
        return self.prices[symbol]

    @staticmethod
    def _simulate_volume(symbol: str) -> int:
        """Return a plausible trade volume for one tick."""
        # Crypto vs equity have very different volume scales
        if "-USD" in symbol:
            return random.randint(1, 50)
        return random.randint(100, 50_000)


# ---------------------------------------------------------------------------
# IoT / Sensor data generator (bonus)
# ---------------------------------------------------------------------------

class SensorDataGenerator:
    """
    Generates simulated IoT sensor readings (temperature, pressure, humidity).

    Useful for demonstrating non-financial streaming use-cases.
    """

    SENSORS = ["sensor_01", "sensor_02", "sensor_03", "sensor_04", "sensor_05"]

    def __init__(self, sensors: Optional[list[str]] = None) -> None:
        self.sensors = sensors or self.SENSORS
        self._temps = {s: random.uniform(18, 25) for s in self.sensors}
        self._pressures = {s: random.uniform(1000, 1025) for s in self.sensors}
        self._humidities = {s: random.uniform(40, 70) for s in self.sensors}

    def generate_reading(self, sensor_id: Optional[str] = None) -> dict:
        """Return one sensor reading."""
        sid = sensor_id or random.choice(self.sensors)

        self._temps[sid] += np.random.normal(0, 0.1)
        self._pressures[sid] += np.random.normal(0, 0.5)
        self._humidities[sid] += np.random.normal(0, 0.3)

        # Clamp to realistic ranges
        self._temps[sid] = max(-20, min(80, self._temps[sid]))
        self._pressures[sid] = max(950, min(1050, self._pressures[sid]))
        self._humidities[sid] = max(0, min(100, self._humidities[sid]))

        return {
            "sensor_id": sid,
            "temperature_c": round(self._temps[sid], 2),
            "pressure_hpa": round(self._pressures[sid], 2),
            "humidity_pct": round(self._humidities[sid], 2),
            "status": self._status(self._temps[sid]),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    def stream(self, interval: float = 0.5) -> Generator[dict, None, None]:
        while True:
            for sid in self.sensors:
                yield self.generate_reading(sid)
            time.sleep(interval)

    @staticmethod
    def _status(temp: float) -> str:
        if temp > 60:
            return "CRITICAL"
        if temp > 40:
            return "WARNING"
        return "OK"


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    gen = MarketDataGenerator()
    print("=== Market Ticks ===")
    for tick in gen.stream():
        print(json.dumps(tick, indent=2))
        if tick["tick_id"] >= 5:
            break

    print("\n=== Sensor Readings ===")
    sensor_gen = SensorDataGenerator()
    for i, reading in enumerate(sensor_gen.stream()):
        print(json.dumps(reading, indent=2))
        if i >= 4:
            break
