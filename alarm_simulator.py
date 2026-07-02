import asyncio
import json
import logging
import random
import signal
import uuid
from datetime import datetime, timezone
from typing import Any

from faker import Faker
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


# -----------------------------
# Configuration
# -----------------------------
HOST = "localhost"
PORT = 8765

# Number of alarms per simulated fault storm.
MIN_STORM_SIZE = 50
MAX_STORM_SIZE = 100

# Time between individual alarm events, in seconds.
EVENT_INTERVAL_SECONDS = 0.15

# Time between fault storms, in seconds.
STORM_INTERVAL_SECONDS = 3

# Probability that a storm includes an unknown anomaly.
ANOMALY_PROBABILITY = 0.20


# -----------------------------
# Logging and shared state
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

fake = Faker()
connected_clients: set[ServerConnection] = set()
shutdown_event = asyncio.Event()


# -----------------------------
# Static topology data
# -----------------------------
TOPOLOGY = {
    "RAN": [
        {"node_id": "RAN-CELL-001", "node_name": "Cell-A"},
        {"node_id": "RAN-CELL-002", "node_name": "Cell-B"},
        {"node_id": "RAN-CELL-003", "node_name": "Cell-C"},
    ],
    "TRANSPORT": [
        {"node_id": "TRANSPORT-LINK-001", "node_name": "Backhaul-Link-1"},
        {"node_id": "TRANSPORT-LINK-002", "node_name": "Backhaul-Link-2"},
        {"node_id": "TRANSPORT-LINK-003", "node_name": "Aggregation-Link-1"},
    ],
    "CORE": [
        {"node_id": "CORE-AMF-001", "node_name": "AMF-1"},
        {"node_id": "CORE-SMF-001", "node_name": "SMF-1"},
    ],
    "EPC": [
        {"node_id": "EPC-MME-001", "node_name": "MME-1"},
        {"node_id": "EPC-SGW-001", "node_name": "SGW-1"},
    ],
}

FAULT_SCENARIOS = [
    {
        "fault_code": "RAN_RF_FAILURE",
        "domain": "RAN",
        "severity": "CRITICAL",
        "message": "Radio-frequency failure detected",
    },
    {
        "fault_code": "TRANSPORT_LINK_DOWN",
        "domain": "TRANSPORT",
        "severity": "MAJOR",
        "message": "Transport link unavailable",
    },
    {
        "fault_code": "CORE_SERVICE_DEGRADED",
        "domain": "CORE",
        "severity": "MAJOR",
        "message": "Core service response time degraded",
    },
    {
        "fault_code": "EPC_SIGNALING_FAILURE",
        "domain": "EPC",
        "severity": "CRITICAL",
        "message": "EPC signaling failure detected",
    },
    {
        "fault_code": "HIGH_PACKET_LOSS",
        "domain": "TRANSPORT",
        "severity": "WARNING",
        "message": "Packet loss threshold exceeded",
    },
    {
        "fault_code": "CELL_CONGESTION",
        "domain": "RAN",
        "severity": "WARNING",
        "message": "Cell congestion threshold exceeded",
    },
]


def utc_timestamp() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def make_alarm(
    *,
    storm_id: str,
    alarm_role: str,
    domain: str,
    node: dict[str, str],
    fault_code: str,
    severity: str,
    message: str,
    parent_alarm_id: str | None = None,
    is_anomaly: bool = False,
) -> dict[str, Any]:
    """Build one JSON-serializable alarm event."""
    return {
        "event_id": str(uuid.uuid4()),
        "storm_id": storm_id,
        "parent_alarm_id": parent_alarm_id,
        "timestamp": utc_timestamp(),
        "alarm_role": alarm_role,  # PARENT, CHILD, or ANOMALY
        "domain": domain,
        "node_id": node["node_id"],
        "node_name": node["node_name"],
        "fault_code": fault_code,
        "severity": severity,
        "message": message,
        "source_system": "alarm-data-simulator",
        "is_anomaly": is_anomaly,
    }


def generate_fault_storm(storm_size: int) -> list[dict[str, Any]]:
    """
    Generate one parent fault and multiple correlated child alarms.
    Optionally add one unknown anomaly.
    """
    scenario = random.choice(FAULT_SCENARIOS)
    storm_id = str(uuid.uuid4())

    parent_node = random.choice(TOPOLOGY[scenario["domain"]])

    parent_alarm = make_alarm(
        storm_id=storm_id,
        alarm_role="PARENT",
        domain=scenario["domain"],
        node=parent_node,
        fault_code=scenario["fault_code"],
        severity=scenario["severity"],
        message=scenario["message"],
    )

    alarms = [parent_alarm]

    # Child alarms may occur in the same or adjacent network domains.
    available_domains = list(TOPOLOGY.keys())

    for _ in range(storm_size - 1):
        child_domain = random.choice(available_domains)
        child_node = random.choice(TOPOLOGY[child_domain])

        child_alarm = make_alarm(
            storm_id=storm_id,
            alarm_role="CHILD",
            domain=child_domain,
            node=child_node,
            fault_code=f"IMPACT_{scenario['fault_code']}",
            severity=random.choice(["WARNING", "MINOR", "MAJOR"]),
            message=f"Service impact correlated with {scenario['fault_code']}",
            parent_alarm_id=parent_alarm["event_id"],
        )

        alarms.append(child_alarm)

    # Add an unknown event for the Isolation Forest stage.
    if random.random() < ANOMALY_PROBABILITY:
        anomaly_domain = random.choice(available_domains)
        anomaly_node = random.choice(TOPOLOGY[anomaly_domain])

        anomaly = make_alarm(
            storm_id=str(uuid.uuid4()),
            alarm_role="ANOMALY",
            domain=anomaly_domain,
            node=anomaly_node,
            fault_code="UNKNOWN_BEHAVIOUR",
            severity="CRITICAL",
            message="Unexpected alarm pattern injected for anomaly detection",
            is_anomaly=True,
        )

        alarms.append(anomaly)

    return alarms


async def register_client(websocket: ServerConnection) -> None:
    """
    Keep the WebSocket connection open.
    The producer task broadcasts events to this client.
    """
    connected_clients.add(websocket)
    client_address = websocket.remote_address

    logging.info("Client connected: %s | total clients: %d",
                 client_address, len(connected_clients))

    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        logging.info("Client disconnected: %s | total clients: %d",
                     client_address, len(connected_clients))


async def broadcast_alarm(alarm: dict[str, Any]) -> None:
    """Broadcast one alarm event to every currently connected client."""
    if not connected_clients:
        return

    payload = json.dumps(alarm)

    disconnected_clients = set()

    for client in connected_clients.copy():
        try:
            await client.send(payload)
        except ConnectionClosed:
            disconnected_clients.add(client)

    connected_clients.difference_update(disconnected_clients)


async def alarm_producer() -> None:
    """Continuously generate alarm storms and broadcast their events."""
    logging.info("Alarm producer started.")

    while not shutdown_event.is_set():
        storm_size = random.randint(MIN_STORM_SIZE, MAX_STORM_SIZE)
        storm = generate_fault_storm(storm_size)

        logging.info(
            "Generated storm: %s | events: %d",
            storm[0]["storm_id"],
            len(storm),
        )

        for alarm in storm:
            if shutdown_event.is_set():
                break

            await broadcast_alarm(alarm)
            await asyncio.sleep(EVENT_INTERVAL_SECONDS)

        await asyncio.sleep(STORM_INTERVAL_SECONDS)


async def main() -> None:
    """Start the WebSocket server and the shared alarm producer."""
    async with serve(register_client, HOST, PORT):
        logging.info("WebSocket server running at ws://%s:%d", HOST, PORT)

        producer_task = asyncio.create_task(alarm_producer())

        await shutdown_event.wait()

        producer_task.cancel()

        try:
            await producer_task
        except asyncio.CancelledError:
            pass


def request_shutdown() -> None:
    """Allow Ctrl+C to stop the application cleanly."""
    logging.info("Shutdown requested.")
    shutdown_event.set()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:
            # Windows may not support add_signal_handler for all signals.
            pass

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        request_shutdown()
    finally:
        loop.close()