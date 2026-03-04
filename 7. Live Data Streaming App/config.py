"""
Configuration for the Live Data Streaming App.
Controls Kafka settings, data generation, and dashboard behaviour.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Kafka settings (used when USE_KAFKA=true in .env or environment)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_MARKET = os.getenv("KAFKA_TOPIC_MARKET", "market-data")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "market-alerts")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "streaming-app-group")

# Set to "true" to use Kafka; any other value falls back to in-memory queues.
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Data-generation settings
# ---------------------------------------------------------------------------
SYMBOLS = os.getenv(
    "SYMBOLS",
    "AAPL,GOOGL,MSFT,AMZN,TSLA,META,NVDA,JPM,BTC-USD,ETH-USD",
).split(",")

# Seconds between ticks for each symbol
TICK_INTERVAL = float(os.getenv("TICK_INTERVAL", "0.5"))

# Intraday price-volatility per tick (as a fraction of current price)
VOLATILITY = float(os.getenv("VOLATILITY", "0.002"))

# Starting prices (USD)
INITIAL_PRICES: dict[str, float] = {
    "AAPL": 182.50,
    "GOOGL": 140.25,
    "MSFT": 375.80,
    "AMZN": 178.90,
    "TSLA": 248.50,
    "META": 495.30,
    "NVDA": 875.20,
    "JPM": 195.60,
    "BTC-USD": 67_000.00,
    "ETH-USD": 3_500.00,
}

# ---------------------------------------------------------------------------
# Dashboard / display settings
# ---------------------------------------------------------------------------
# Maximum ticks kept in memory per symbol for charting
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "200"))

# Dashboard auto-refresh interval in milliseconds
REFRESH_INTERVAL_MS = int(os.getenv("REFRESH_INTERVAL_MS", "1000"))

# Price-change thresholds for colour-coding (percentage)
ALERT_THRESHOLD_HIGH = float(os.getenv("ALERT_THRESHOLD_HIGH", "1.5"))
ALERT_THRESHOLD_LOW = float(os.getenv("ALERT_THRESHOLD_LOW", "-1.5"))
