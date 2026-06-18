# 🧠 Hippocampus — Short-Term Memory System

An MQTT-based short-term data memory service for the **Teleonome** organism. Named after the
brain structure responsible for short-term memory formation. Hippocampus is one "organ" in a
larger Teleonome system, running as a standalone Java process on a Raspberry Pi.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│   Teleonome Heart           External MQTT Broker          Hippocampus    │
│   (other organs)            (e.g. Mosquitto)               (this app)    │
│                              tcp://localhost:1883                        │
│   Status (heartbeat) ───────────────────────────────►  absorbPulse()     │
│   Hippocampus_Request ──────────────────────────────►  processRequest() │
│                                                                          │
│                          ◄────────── Hippocampus_Response                │
│                          ◄────────── Hippocampus_Response/{requestId}    │
│                          ◄────────── Hippocampus_Status (every 30s)      │
│                                                                          │
│                                      ┌──────────────────────┐            │
│                                      │ shortTermMemory       │            │
│                                      │ ConcurrentHashMap<     │            │
│                                      │   identity,            │            │
│                                      │   TreeMap<epochSecs,   │            │
│                                      │     value>>            │            │
│                                      └──────────────────────┘            │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

Hippocampus does **not** embed its own MQTT broker — it connects as a client to an external
broker (Mosquitto or similar) already running at `tcp://localhost:1883`. The `moquette-broker`
Maven dependency is present but unused by the code.

---

## Core Data Structure

```java
ConcurrentHashMap<String, TreeMap<Long, Object>> shortTermMemory
```

- **Key**: a Denome identity pointer + DeneWord name, e.g.
  `@ChinampaMonitor:Telepathons:Chinampa:Purpose:SomeValue`
- **Value**: `TreeMap<epochSeconds, value>` — ordered by time, giving O(log n) inserts and
  O(log n) range queries on the timestamp axis (seconds resolution, not milliseconds).

See `src/main/java/com/teleonome/hippocampus/Hippocampus.java` (single class, ~680 lines) for
the full implementation.

---

## MQTT Topics

| Direction | Topic | Purpose |
|-----------|-------|---------|
| Subscribe | `Status` | Heartbeat pulses from the Teleonome heart — full Denome JSON, processed by `absorbPulse()` |
| Subscribe | `Hippocampus_Request` | Query requests, processed by `processRequest()` |
| Publish   | `Hippocampus_Response` | `"Preload Complete"` notice, sent once after startup preload finishes |
| Publish   | `Hippocampus_Response/{requestId}` | Query results for a given request |
| Publish   | `Hippocampus_Status` | Periodic memory health broadcast (every 30s, via `PingThread`) |

---

## Data Flow

### Ingest — `absorbPulse(payload)`

Triggered on every `Status` message. The payload is the full Denome JSON heartbeat. For each
DeneWord pointer listed in the Hippocampus "Data" dene, it resolves the pointed-to value and
its `Seconds Time`, then stores it in `shortTermMemory` keyed by `pointer:deneWordName`. After
each insert it applies a rolling-window prune (drops anything older than `memoryWindowDays`
for that identity) and calls `checkMemoryHealth()`.

### Query — `processRequest(payload)`

Triggered on every `Hippocampus_Request` message. Expected JSON:

```json
{
  "Identity": "@ChinampaMonitor:Telepathons:Chinampa:Purpose:SomeValue",
  "RequestId": "abc123",
  "Range": 3600000
}
```

`Range` is milliseconds of history to return, counted back from now. The handler looks up the
identity's `TreeMap`, takes a `tailMap` slice for the requested range, and publishes the result
to `Hippocampus_Response/{RequestId}`:

```json
{
  "Identity": "@ChinampaMonitor:Telepathons:Chinampa:Purpose:SomeValue",
  "Data": [
    {"timeSeconds": 1700000000, "timeString": "2023-11-14 ...", "Value": "22.5"}
  ],
  "telepathonName": "Chinampa",
  "deneWordName": "SomeValue",
  "RequestId": "abc123"
}
```

If no data is found for the identity, an empty `Data` array is still published so the
requester doesn't hang waiting for a response.

---

## Startup Sequence

1. Connect to the MQTT broker at `tcp://localhost:1883`.
2. Subscribe to `Status` and `Hippocampus_Request`.
3. `loadData()` — preload historical data from PostgreSQL (via `PostgresqlPersistenceManager`),
   day by day, going back `preLoadHours` (from Denome config, falling back to
   `memoryWindowDays * 24`).
4. `performPostLoadCleanup()` — trim everything to the `memoryWindowDays` rolling window.
5. Publish `"Preload Complete"` to `Hippocampus_Response`.
6. Start the daemon `PingThread`, which every 30s writes
   `/home/pi/Teleonome/HippocampusStatus.json` and publishes the same status to
   `Hippocampus_Status`.

---

## Memory Management

- **Global point limit**: 300,000 (`globalLimit`), overridable via Denome config.
- **Warning threshold**: 270,000 (`warningThreshold`), overridable via Denome config.
- **Rolling window**: `memoryWindowDays` (default 7, from `hippocampus.properties`) — applied
  per identity on every `absorbPulse` insert and again as a one-time sweep after preload.
- **Emergency pruning** (`emergencyPrune()`): once the global limit is hit, removes the single
  oldest point from *every* identity, one pass at a time, until back under the limit.

The `Hippocampus_Status` payload includes a `MemoryBreakdown` (top 15 DeneChains by point
count, plus an `Other` bucket), current/available point counts, and a recommended `-Xmx`
heap size derived from observed usage plus sacrificed points.

---

## Configuration

### `hippocampus.properties`

Read from `/home/pi/Teleonome/lib/hippocampus.properties` at startup (a sample copy lives at
`lib/hippocampus.properties` in this repo for reference — it is **not** the file actually
loaded at runtime).

| Property | Default | Description |
|----------|---------|-------------|
| `memory.window.days` | `7` | Rolling memory window; also derives `preLoadHours = days * 24` |

Missing file falls back to the default silently.

### Denome config

Read from `Teleonome.denome` (`Utils.getLocalDirectory()`) during preload. Can override
`globalLimit` and `preLoadHours`, taking precedence over the properties file.

### Logging

Configured from `/home/pi/Teleonome/lib/Log4J.properties` at runtime, using the legacy
`org.apache.log4j` (1.x) API.

---

## Build & Run

Maven child module of the `organbuilder` parent POM (`../organbuilder/pom.xml`).

```bash
# Build fat JAR (output goes to ../../Hippocampus.jar)
mvn clean package

# Run (must be on the Raspberry Pi host or have the /home/pi/Teleonome/ directory structure)
java -jar ../../Hippocampus.jar

# Run tests
mvn test
```

### Prerequisites

- Java 8 (the pom's `maven.compiler.source/target` says 11, but `maven-compiler-plugin` pins
  the actual compile target to 1.8)
- Maven 3.x
- An external MQTT broker (e.g. Mosquitto) listening on `tcp://localhost:1883`
- PostgreSQL reachable via the Teleonome framework's `PostgresqlPersistenceManager`
- `/home/pi/Teleonome/` directory structure (Denome file, properties, Log4J config) present at
  runtime

---

## Runtime Assumptions

- Hardcoded paths target a Raspberry Pi: `/home/pi/Teleonome/`. Files written at runtime:
  `HippocampusStatus.json`, `Preload.txt`.
- Timezone is hardcoded to `Australia/Melbourne`.
- PostgreSQL is used for historical preload only; all live data arrives via MQTT.
- `System.gc()` is called explicitly on every `absorbPulse` (intentional, to manage heap on
  constrained hardware).
