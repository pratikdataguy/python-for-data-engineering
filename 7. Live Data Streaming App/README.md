# 7. Live Data Streaming App

A **production-ready live data streaming application** built with Python, demonstrating the complete producer → message-bus → consumer → dashboard pipeline used in real data-engineering systems.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│              Live Streaming App                  │
│                                                  │
│  ┌────────────┐   ticks    ┌──────────────────┐  │
│  │  Producer  │ ─────────► │    Message Bus   │  │
│  │ (generator)│            │  Queue / Kafka   │  │
│  └────────────┘            └────────┬─────────┘  │
│                                     │ ticks      │
│                                     ▼            │
│                            ┌────────────────┐    │
│                            │    Consumer    │    │
│                            │  + Analytics  │    │
│                            └───────┬────────┘    │
│                                    │ enriched    │
│                                    ▼            │
│                          ┌──────────────────┐   │
│                          │  Streamlit       │   │
│                          │  Live Dashboard  │   │
│                          └──────────────────┘   │
└──────────────────────────────────────────────────┘
```

---

## Features

- **Simulated market-data feed** — realistic price movements via Geometric Brownian Motion
- **IoT sensor-data generator** — temperature, pressure, humidity for non-financial use-cases
- **Dual transport** — in-memory `queue.Queue` (zero setup) **or** Apache Kafka (production)
- **Rolling analytics** — SMA-20, SMA-50, VWAP, log-return volatility
- **Price alerts** — configurable surge/drop thresholds
- **Streamlit dashboard** — live charts (line or candlestick), volume bars, MA overlays, alert log
- **CSV export** — download any snapshot from the dashboard
- **Kafka UI** — visual topic/consumer-group browser via Docker Compose

---

## Quick start (no Kafka required)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) copy and edit configuration
cp .env.example .env

# 3. Launch the live dashboard
streamlit run streaming_dashboard.py
```

Open **http://localhost:8501** in your browser.

---

## Running the producer / consumer standalone

```bash
# Terminal 1 — watch raw ticks
python producer.py

# Terminal 2 — watch enriched ticks + analytics
python consumer.py
```

---

## Optional: Kafka mode

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) or Docker + Docker Compose

### Steps

```bash
# Start Kafka + Zookeeper + Kafka UI
docker compose up -d

# Enable Kafka in your config
cp .env.example .env
# Edit .env → set USE_KAFKA=true

# Run with Kafka
python producer.py
python consumer.py
streamlit run streaming_dashboard.py
```

Kafka UI is available at **http://localhost:8080**.

---

## Project structure

```
7. Live Data Streaming App/
├── config.py                       # All tuneable settings (reads .env)
├── data_generator.py               # Market & IoT data generators
├── producer.py                     # In-memory and Kafka producers
├── consumer.py                     # Consumer + rolling analytics engine
├── streaming_dashboard.py          # Streamlit live dashboard
├── docker-compose.yml              # Kafka stack
├── .env.example                    # Configuration template
├── requirements.txt                # Python dependencies
└── Live Data Streaming App.ipynb   # Step-by-step tutorial notebook
```

---

## Configuration reference

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_KAFKA` | `false` | Switch to Kafka transport |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `SYMBOLS` | `AAPL,GOOGL,…` | Symbols to stream |
| `TICK_INTERVAL` | `0.3` | Seconds between tick rounds |
| `VOLATILITY` | `0.002` | Per-tick price volatility (0.2%) |
| `MAX_HISTORY` | `200` | Ticks kept in memory per symbol |
| `REFRESH_INTERVAL_MS` | `1000` | Dashboard refresh rate |
| `ALERT_THRESHOLD_HIGH` | `1.5` | Surge alert threshold (%) |
| `ALERT_THRESHOLD_LOW` | `-1.5` | Drop alert threshold (%) |

---

## Key concepts covered

| Concept | File |
|---------|------|
| Producer / Consumer pattern | `producer.py`, `consumer.py` |
| Thread-safe queue | `producer.py` (`queue.Queue`) |
| Geometric Brownian Motion | `data_generator.py` |
| Rolling SMA / VWAP | `consumer.py` (`StreamAnalytics`) |
| Log-return volatility | `consumer.py` (`StreamAnalytics`) |
| Streamlit + Plotly | `streaming_dashboard.py` |
| Apache Kafka | `docker-compose.yml`, Kafka classes |
| Environment config | `config.py`, `.env.example` |
