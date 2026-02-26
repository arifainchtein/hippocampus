# 🧠 Hippocampus — Short-Term Memory System

A distributed MQTT-based short-term data memory system. Named after the brain structure
responsible for short-term memory formation.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   Data Sources            Moquette Broker            Consumers      │
│   ─────────────           ──────────────────         ──────────     │
│                           Port 1883 (TCP)                           │
│   Any publisher  ──────►  Port 8083 (WS)  ◄───────  Web App        │
│   (sensors, apps)         │                          (jQuery/PAHO)  │
│                           │                                         │
│                           ▼                                         │
│                    ┌──────────────┐                                 │
│                    │ Hippocampus  │  The Memory                     │
│                    │  (Java App)  │                                 │
│                    │              │                                 │
│                    │  TreeMap<    │                                 │
│                    │   identity,  │                                 │
│                    │   TreeMap<   │                                 │
│                    │    epoch,    │                                 │
│                    │    DataPoint │                                 │
│                    │   >>         │                                 │
│                    └──────────────┘                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. `hippocampus/` — The Memory Service (Java)

The core service. Subscribes to MQTT, stores data in a **two-level TreeMap**:

```
identity  →  TreeMap<epochTimestamp(ms), DataPoint>
```

This gives **O(log n) inserts and O(log n) range queries** on the timestamp axis.

**Key Classes:**
- `HippocampusApp`   — main entry point, wires everything together
- `MemoryStore`      — the TreeMap memory (thread-safe, with eviction)
- `MqttHandler`      — subscribes/publishes via Eclipse PAHO
- `DataPoint`        — identity + epochTimestamp + value
- `QueryRequest`     — query envelope (type, identity, fromEpoch, toEpoch)

**MQTT Topics:**

| Direction | Topic | Purpose |
|-----------|-------|---------|
| Subscribe | `data/ingest` | Receive new DataPoints |
| Subscribe | `hippocampus/query` | Receive query requests |
| Publish   | `hippocampus/response/{corrId}` | Send query results |
| Publish   | `hippocampus/status` | Periodic heartbeat + stats |

---

### 2. `broker/` — Moquette MQTT Broker (Java)

An embedded Moquette broker. Runs as a standalone JAR.

- **TCP port 1883** — standard MQTT (for Hippocampus app, backend publishers)
- **WebSocket port 8083** (`/mqtt`) — for browser-based PAHO client

Configure via `src/main/resources/moquette.conf`.

---

### 3. `webapp/` — Web Console (Tomcat + jQuery + PAHO)

A WAR deployed to Tomcat. Pure HTML/JS — no server-side processing needed.

- Connects to broker via **PAHO over WebSocket** (`ws://localhost:8083/mqtt`)
- **Identity buttons** with `data-identity` attributes select what to query
- Query types: Latest, Range, All, Stats, All Identities
- **Quick Range presets**: Midnight→Now, Midnight→8am, Last 1h, Last 24h
- **Publish test data** panel for testing
- Live **MQTT activity log**
- Auto-discovers new identities from `hippocampus/status` messages

---

## Data Model

### DataPoint (JSON)

```json
{
  "identity"       : "temperature-sensor-1",
  "epochTimestamp" : 1700000000000,
  "value"          : "22.5"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `identity` | String | Uniquely identifies what data type this is |
| `epochTimestamp` | long | Unix epoch in **milliseconds** when data was recorded |
| `value` | String | The data value (stored as string, interpret as needed) |

### Ingest (publish to `data/ingest`)

Single point:
```json
{"identity":"temperature-sensor-1","epochTimestamp":1700000000000,"value":"22.5"}
```

Batch (array):
```json
[
  {"identity":"temperature-sensor-1","epochTimestamp":1700000000000,"value":"22.5"},
  {"identity":"humidity-sensor-1","epochTimestamp":1700000000001,"value":"65.3"}
]
```

### Query Request (publish to `hippocampus/query`)

```json
{
  "correlationId" : "abc123",
  "queryType"     : "RANGE",
  "identity"      : "temperature-sensor-1",
  "fromEpoch"     : 1700000000000,
  "toEpoch"       : 1700086400000
}
```

**Query Types:** `LATEST` | `RANGE` | `ALL` | `STATS` | `IDENTITIES`

### Query Response (received on `hippocampus/response/{correlationId}`)

```json
{
  "correlationId" : "abc123",
  "queryType"     : "RANGE",
  "identity"      : "temperature-sensor-1",
  "timestamp"     : 1700090000000,
  "result"        : [...],
  "count"         : 42,
  "fromEpoch"     : 1700000000000,
  "toEpoch"       : 1700086400000
}
```

---

## Getting Started

### Prerequisites

- Java 11+
- Maven 3.8+
- Tomcat 9+ (for webapp)

### Build

```bash
# Build Hippocampus
cd hippocampus && mvn clean package -q

# Build Broker
cd broker && mvn clean package -q

# Build Webapp
cd webapp && mvn clean package -q
```

### Run

**Step 1: Start the broker**
```bash
java -jar broker/target/hippocampus-broker-1.0.0-jar-with-dependencies.jar
```

**Step 2: Start the Hippocampus**
```bash
# Default: connects to tcp://localhost:1883
java -jar hippocampus/target/hippocampus-1.0.0-jar-with-dependencies.jar

# Custom broker URL:
java -Dbroker.url=tcp://mybroker:1883 \
     -Dmemory.maxPoints=50000 \
     -jar hippocampus/target/hippocampus-1.0.0-jar-with-dependencies.jar
```

**Step 3: Deploy webapp to Tomcat**
```bash
cp webapp/target/hippocampus-webapp.war $TOMCAT_HOME/webapps/
```

Then open: `http://localhost:8080/hippocampus-webapp/`

---

## Configuration

### Hippocampus System Properties

| Property | Default | Description |
|----------|---------|-------------|
| `broker.url` | `tcp://localhost:1883` | MQTT broker TCP URL |
| `memory.maxPoints` | `10000` | Max DataPoints stored per identity (FIFO eviction) |
| `status.intervalSec` | `30` | Heartbeat publish interval in seconds |

### Moquette (broker/src/main/resources/moquette.conf)

| Setting | Default | Description |
|---------|---------|-------------|
| `port` | `1883` | TCP MQTT port |
| `websocket_port` | `8083` | WebSocket MQTT port (for browser PAHO) |
| `allow_anonymous` | `true` | Allow unauthenticated connections |

---

## Example: Querying midnight-to-8am data from the webapp

1. Click the identity button (e.g., `temperature-sensor-1`)
2. Click **"Midnight → 8am"** quick range preset
3. Verify the From/To fields show today 00:00 → 08:00
4. Click **Run Query**
5. Results appear in the table, sorted by timestamp

Behind the scenes, the webapp:
1. Publishes to `hippocampus/query`:
   ```json
   {"correlationId":"abc123","queryType":"RANGE","identity":"temperature-sensor-1",
    "fromEpoch":1700000000000,"toEpoch":1700028800000}
   ```
2. The Hippocampus runs `memoryStore.getRange("temperature-sensor-1", midnight, 8am)`
3. Responds on `hippocampus/response/abc123` with matching DataPoints
4. The webapp receives via PAHO subscription and renders the table

---

## Extending the System

**Adding a new identity button in the webapp:**
```html
<button class="identity-btn" data-identity="my-sensor-id" onclick="selectIdentity(this)">
    my-sensor-id
    <small>Description of this data source</small>
</button>
```

**Publishing data from any MQTT client:**
```bash
# Using mosquitto_pub:
mosquitto_pub -h localhost -p 1883 \
  -t data/ingest \
  -m '{"identity":"my-sensor","epochTimestamp":1700000000000,"value":"42"}'
```
