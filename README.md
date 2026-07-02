**AIProject - Autonomous Networks**

## Prototype implementation summary so far

### 1. Set up the Python environment

Create a project folder:

```bash
mkdir AIProject
cd AIProject
```

Create and activate a virtual environment.

**macOS/Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows PowerShell**

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the required packages:

```bash
python -m pip install --upgrade pip
python -m pip install faker websockets
```

`Faker` generates realistic synthetic alarm metadata. `websockets` creates real-time communication between components.

---

### 2. Build the alarm data simulator

Create:

```
alarm_simulator.py
```

Its responsibilities are:

* Generate synthetic telecom alarms.
* Create a parent/root-cause alarm.
* Create approximately 50–100 child/impact alarms.
* Optionally inject unknown anomalies.
* Attach a shared `storm_id` to alarms belonging to the same incident.
* Broadcast alarms over WebSocket.

The simulator runs at:

```
ws://localhost:8765
```

Start it using:

```
python alarm_simulator.py
```

The server output:

```
WebSocket server running at ws://localhost:8765
Alarm producer started.
Generated storm: <storm-id> | events: <number>
```

means:

* The WebSocket server is active.
* Alarm generation has started.
* One simulated network incident has been created.
* The generated alarms are being sent to connected clients.

The simulator sends one JSON alarm event at a time. A typical event includes:

```json
{
  "event_id": "unique-event-id",
  "storm_id": "shared-incident-id",
  "parent_alarm_id": "root-event-id",
  "timestamp": "2026-07-02T10:20:14+00:00",
  "alarm_role": "PARENT",
  "domain": "RAN",
  "node_id": "RAN-CELL-001",
  "fault_code": "RAN_RF_FAILURE",
  "severity": "CRITICAL",
  "message": "Radio-frequency failure detected",
  "is_anomaly": false
}
```

### 3. Test the alarm simulator output

Create:

```
test_client.py
```

This connects to:

```
ws://localhost:8765
```

and prints incoming alarms in the terminal.

Run it in a second terminal:

```bash
python test_client.py
```

The architecture at this stage is:

```
alarm_simulator.py
        │
        │ WebSocket JSON events
        ▼
ws://localhost:8765
        │
        ▼
test_client.py
```

The test client confirms that the simulator is producing and broadcasting alarm data correctly.

---

### 4. Create the Netcool emulator

Create:

```
netcool_emulator.py
```

The Netcool emulator acts as a deterministic rule-based correlation layer.

It receives raw alarms from:

```
ws://localhost:8765
```

It applies static parent-child rules, such as:

| Parent/root-cause fault | Child/impact fault             |
| ----------------------- | ------------------------------ |
| `RAN_RF_FAILURE`        | `IMPACT_RAN_RF_FAILURE`        |
| `TRANSPORT_LINK_DOWN`   | `IMPACT_TRANSPORT_LINK_DOWN`   |
| `CORE_SERVICE_DEGRADED` | `IMPACT_CORE_SERVICE_DEGRADED` |
| `EPC_SIGNALING_FAILURE` | `IMPACT_EPC_SIGNALING_FAILURE` |
| `HIGH_PACKET_LOSS`      | `IMPACT_HIGH_PACKET_LOSS`      |
| `CELL_CONGESTION`       | `IMPACT_CELL_CONGESTION`       |


The Netcool emulator processes buffered alarms every 5 seconds, but retains parent alarms for 60 seconds. This lets child alarms arriving later still be associated with their root cause.

---

### 5. Run the Netcool emulator

Create the output directory:

```bash
mkdir output
```

Start the simulator first:

```bash
python alarm_simulator.py
```

Then start the Netcool emulator in a second terminal:

```bash
python netcool_emulator.py
```

The Netcool emulator:

* Connects to the simulator.
* Receives raw alarms.
* Stores parent alarms temporarily.
* Matches child alarms against static rules.
* Labels alarms as `ROOT_CAUSE`, `CORRELATED_CHILD`, or `UNMATCHED`.
* Writes processed data to a file.
* Broadcasts processed events to downstream systems.

It exposes its own WebSocket endpoint:

```
ws://localhost:8766
```

The full pipeline is now:

```
alarm_simulator.py
        │
        │ Raw alarms
        ▼
ws://localhost:8765
        │
        ▼
netcool_emulator.py
        │
        ├── output/netcool_events.jsonl
        │
        └── ws://localhost:8766
                │
                ▼
Future ML engine / Streamlit dashboard
```

---

### 6. Netcool correlation output states

Each alarm receives a Netcool-style status.

| Status             | Meaning                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------- |
| `ROOT_CAUSE`       | A recognised parent alarm, such as `RAN_RF_FAILURE`.                                         |
| `CORRELATED_CHILD` | A child alarm matched to a parent using a static rule and the 45-second time window.         |
| `UNMATCHED`        | No static rule could identify a root cause. This is especially useful for anomaly detection. |

Example output:

```json
{
  "event_id": "child-event-id",
  "fault_code": "IMPACT_RAN_RF_FAILURE",
  "netcool_status": "CORRELATED_CHILD",
  "netcool_root_cause_event_id": "parent-event-id",
  "netcool_root_cause_fault_code": "RAN_RF_FAILURE",
  "netcool_correlation_method": "STATIC_RULE",
  "netcool_correlation_window_seconds": 45
}
```

---

### 7. Test the Netcool emulator output

Create:

```
netcool_test_client.py
```

This connects to:

```
ws://localhost:8766
```

Run it in a third terminal:

```bash
python netcool_test_client.py
```

Expected style of output:

```
ROOT_CAUSE         | RAN_RF_FAILURE                | root cause: RAN_RF_FAILURE
CORRELATED_CHILD   | IMPACT_RAN_RF_FAILURE         | root cause: RAN_RF_FAILURE
UNMATCHED          | UNKNOWN_BEHAVIOUR             | root cause: None
```

This confirms that the static rule engine is working.

---

### 8. Save processed alarms as JSON Lines

The Netcool emulator creates:

```
output/netcool_events.jsonl
```

**macOS/Linux**

```bash
cat output/netcool_events.jsonl
```

**Windows PowerShell**

```powershell
Get-Content output\netcool_events.jsonl
```

Load it later using pandas:

```python
import pandas as pd

df = pd.read_json(
    "output/netcool_events.jsonl",
    lines=True
)

print(df.head())
```

---

### 9. Current prototype status

You have now completed these components:

```
[Completed] Python environment
[Completed] WebSocket library installation
[Completed] Synthetic alarm generator
[Completed] Parent-child alarm storm generation
[Completed] WebSocket broadcast server
[Completed] Simulator test client
[Completed] Static Netcool-style rule engine
[Completed] 45-second correlation window
[Completed] JSON Lines output storage
[Completed] Netcool output WebSocket server
[Completed] Netcool test client
[Completed] AI/ML correlation engine using DBSCAN
[Completed] Isolation Forest anomaly detection
[Completed] NetworkX multi-domain topology graph
```

The next implementation stages are:

```
[Next] Streamlit dashboard
[Next] D3.js topology visualisation
[Next] Metrics dashboard using Chart.js
[Next] End-to-end comparison: Netcool static rules vs AI correlation
```

