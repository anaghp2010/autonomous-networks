import asyncio
import json
import logging
import random
import signal
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


# ============================================================
# Configuration
# ============================================================

HOST = "localhost"
PORT = 8765

# Number of related alarms generated for each incident.
MIN_STORM_SIZE = 25
MAX_STORM_SIZE = 70

# Time between WebSocket messages. This is intentionally short so
# you can generate enough data for DBSCAN and Isolation Forest.
EVENT_INTERVAL_SECONDS = 0.12
STORM_INTERVAL_SECONDS = 4

# Realism controls.
MISSING_PARENT_PROBABILITY = 0.18
DUPLICATE_PROBABILITY = 0.15
UNRELATED_ALARM_PROBABILITY = 0.30
ANOMALY_PROBABILITY = 0.12
NOISY_FAULT_CODE_PROBABILITY = 0.35

# Child alarms can be delayed by up to this many seconds.
# The timestamp is delayed even though the demo sends events quickly.
MAX_CHILD_DELAY_SECONDS = 90

# Number of unrelated background alarms injected per storm.
MIN_BACKGROUND_ALARMS = 2
MAX_BACKGROUND_ALARMS = 8


# ============================================================
# Logging and shared state
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

fake = Faker()
connected_clients: set[ServerConnection] = set()
shutdown_event = asyncio.Event()


# ============================================================
# Multi-domain topology
# ============================================================

# Each node has a domain and a logical topology level.
# Lower/higher level differences are later useful as topology-distance
# features for the correlation engine.
TOPOLOGY_NODES = {
    "RAN-CELL-001": {
        "node_name": "Cell-A",
        "domain": "RAN",
        "level": 0,
    },
    "RAN-CELL-002": {
        "node_name": "Cell-B",
        "domain": "RAN",
        "level": 0,
    },
    "RAN-CELL-003": {
        "node_name": "Cell-C",
        "domain": "RAN",
        "level": 0,
    },
    "RAN-GNB-001": {
        "node_name": "gNodeB-1",
        "domain": "RAN",
        "level": 1,
    },
    "TRANSPORT-LINK-001": {
        "node_name": "Backhaul-Link-1",
        "domain": "TRANSPORT",
        "level": 2,
    },
    "TRANSPORT-LINK-002": {
        "node_name": "Backhaul-Link-2",
        "domain": "TRANSPORT",
        "level": 2,
    },
    "TRANSPORT-AGG-001": {
        "node_name": "Aggregation-Router-1",
        "domain": "TRANSPORT",
        "level": 3,
    },
    "CORE-AMF-001": {
        "node_name": "AMF-1",
        "domain": "CORE",
        "level": 4,
    },
    "CORE-SMF-001": {
        "node_name": "SMF-1",
        "domain": "CORE",
        "level": 4,
    },
    "CORE-UPF-001": {
        "node_name": "UPF-1",
        "domain": "CORE",
        "level": 5,
    },
    "EPC-MME-001": {
        "node_name": "MME-1",
        "domain": "EPC",
        "level": 4,
    },
    "EPC-SGW-001": {
        "node_name": "SGW-1",
        "domain": "EPC",
        "level": 5,
    },
}

# Physical/logical connectivity graph.
# The shortest-path length becomes the topology distance.
TOPOLOGY_EDGES = [
    ("RAN-CELL-001", "RAN-GNB-001"),
    ("RAN-CELL-002", "RAN-GNB-001"),
    ("RAN-CELL-003", "RAN-GNB-001"),
    ("RAN-GNB-001", "TRANSPORT-LINK-001"),
    ("RAN-GNB-001", "TRANSPORT-LINK-002"),
    ("TRANSPORT-LINK-001", "TRANSPORT-AGG-001"),
    ("TRANSPORT-LINK-002", "TRANSPORT-AGG-001"),
    ("TRANSPORT-AGG-001", "CORE-AMF-001"),
    ("TRANSPORT-AGG-001", "CORE-SMF-001"),
    ("CORE-AMF-001", "CORE-UPF-001"),
    ("CORE-SMF-001", "CORE-UPF-001"),
    ("CORE-AMF-001", "EPC-MME-001"),
    ("EPC-MME-001", "EPC-SGW-001"),
]

# Pre-calculated shortest path distances.
ADJACENCY: dict[str, set[str]] = {
    node_id: set() for node_id in TOPOLOGY_NODES
}

for source, target in TOPOLOGY_EDGES:
    ADJACENCY[source].add(target)
    ADJACENCY[target].add(source)


# ============================================================
# Fault and propagation definitions
# ============================================================

FAULT_SCENARIOS = [
    {
        "fault_code": "RAN_RF_FAILURE",
        "root_domain": "RAN",
        "severity": "CRITICAL",
        "message": "Radio-frequency failure detected",
        "propagation_domains": ["RAN", "TRANSPORT", "CORE"],
    },
    {
        "fault_code": "TRANSPORT_LINK_DOWN",
        "root_domain": "TRANSPORT",
        "severity": "CRITICAL",
        "message": "Transport link unavailable",
        "propagation_domains": ["TRANSPORT", "RAN", "CORE", "EPC"],
    },
    {
        "fault_code": "CORE_SERVICE_DEGRADED",
        "root_domain": "CORE",
        "severity": "MAJOR",
        "message": "Core service response time degraded",
        "propagation_domains": ["CORE", "EPC", "RAN"],
    },
    {
        "fault_code": "EPC_SIGNALING_FAILURE",
        "root_domain": "EPC",
        "severity": "CRITICAL",
        "message": "EPC signaling failure detected",
        "propagation_domains": ["EPC", "CORE", "RAN"],
    },
    {
        "fault_code": "HIGH_PACKET_LOSS",
        "root_domain": "TRANSPORT",
        "severity": "MAJOR",
        "message": "Packet loss threshold exceeded",
        "propagation_domains": ["TRANSPORT", "RAN", "CORE"],
    },
    {
        "fault_code": "CELL_CONGESTION",
        "root_domain": "RAN",
        "severity": "WARNING",
        "message": "Cell congestion threshold exceeded",
        "propagation_domains": ["RAN", "TRANSPORT"],
    },
]

SEVERITY_ORDER = {
    "CLEAR": 0,
    "WARNING": 1,
    "MINOR": 2,
    "MAJOR": 3,
    "CRITICAL": 4,
}


# ============================================================
# Utility functions
# ============================================================

def utc_timestamp(offset_seconds: float = 0) -> str:
    """Return ISO-8601 UTC time with an optional simulated delay."""
    event_time = datetime.now(timezone.utc) + timedelta(
        seconds=offset_seconds
    )
    return event_time.isoformat()


def nodes_in_domain(domain: str) -> list[str]:
    """Return all topology nodes belonging to one domain."""
    return [
        node_id
        for node_id, node in TOPOLOGY_NODES.items()
        if node["domain"] == domain
    ]


def shortest_topology_distance(
    source_node: str,
    target_node: str,
) -> int:
    """
    Breadth-first search shortest path length.

    Returns a large value if nodes are disconnected.
    """
    if source_node == target_node:
        return 0

    visited = {source_node}
    queue = deque([(source_node, 0)])

    while queue:
        current_node, distance = queue.popleft()

        for neighbour in ADJACENCY[current_node]:
            if neighbour == target_node:
                return distance + 1

            if neighbour not in visited:
                visited.add(neighbour)
                queue.append((neighbour, distance + 1))

    return 99


def choose_node_in_domain(domain: str) -> str:
    """Choose a random node from one domain."""
    return random.choice(nodes_in_domain(domain))


def mutate_fault_code(fault_code: str) -> str:
    """
    Introduce noisy naming variations.

    Examples:
    RAN_RF_FAILURE -> ran-rf-failure
    RAN_RF_FAILURE -> RAN RF FAIL
    RAN_RF_FAILURE -> RAN_RF_FAILURE_V2
    """
    variants = [
        fault_code.lower(),
        fault_code.replace("_", "-"),
        fault_code.replace("_", " "),
        fault_code.replace("FAILURE", "FAIL"),
        fault_code.replace("DEGRADED", "DEGRADATION"),
        f"{fault_code}_V2",
        f"ALARM_{fault_code}",
    ]

    return random.choice(variants)


def change_severity(base_severity: str) -> str:
    """
    Simulate escalation or de-escalation.

    A CRITICAL parent may create MAJOR/MINOR child alarms,
    while a WARNING parent may escalate into MAJOR impact alarms.
    """
    base_level = SEVERITY_ORDER[base_severity]

    adjustment = random.choice([-2, -1, 0, 1, 1])
    new_level = max(0, min(4, base_level + adjustment))

    for severity, level in SEVERITY_ORDER.items():
        if level == new_level:
            return severity

    return base_severity


def make_alarm(
    *,
    storm_id: str | None,
    event_role: str,
    root_cause_event_id: str | None,
    root_cause_fault_code: str | None,
    node_id: str,
    fault_code: str,
    severity: str,
    message: str,
    delay_seconds: float,
    topology_distance_from_root: int | None,
    is_ground_truth_anomaly: bool,
    is_duplicate: bool = False,
    duplicate_of_event_id: str | None = None,
    parent_missing: bool = False,
) -> dict[str, Any]:
    """Create one alarm event with both visible and hidden ground truth."""
    node = TOPOLOGY_NODES[node_id]

    return {
        "event_id": str(uuid.uuid4()),
        "storm_id": storm_id,
        "parent_alarm_id": root_cause_event_id,
        "timestamp": utc_timestamp(delay_seconds),
        "alarm_role": event_role,
        "domain": node["domain"],
        "node_id": node_id,
        "node_name": node["node_name"],
        "fault_code": fault_code,
        "severity": severity,
        "message": message,
        "source_system": "strong-alarm-simulator",

        # Ground-truth fields used later to evaluate AI performance.
        "ground_truth_root_cause_event_id": root_cause_event_id,
        "ground_truth_root_cause_fault_code": root_cause_fault_code,
        "ground_truth_delay_seconds": round(delay_seconds, 2),
        "ground_truth_topology_distance": topology_distance_from_root,
        "ground_truth_parent_missing": parent_missing,
        "ground_truth_is_duplicate": is_duplicate,
        "ground_truth_duplicate_of_event_id": duplicate_of_event_id,
        "ground_truth_is_anomaly": is_ground_truth_anomaly,
    }


def create_duplicate_alarm(original_alarm: dict[str, Any]) -> dict[str, Any]:
    """
    Create a near-duplicate alarm with a new event ID and small delay.
    """
    duplicate = original_alarm.copy()

    duplicate["event_id"] = str(uuid.uuid4())
    duplicate["timestamp"] = utc_timestamp(
        random.uniform(1, 8)
    )
    duplicate["ground_truth_is_duplicate"] = True
    duplicate["ground_truth_duplicate_of_event_id"] = (
        original_alarm["event_id"]
    )
    duplicate["message"] = (
        f"{original_alarm['message']} [duplicate notification]"
    )

    return duplicate


# ============================================================
# Strong incident generator
# ============================================================

def generate_strong_fault_storm() -> list[dict[str, Any]]:
    """
    Generate one realistic multi-domain incident.

    Added realism:
    - delayed children
    - cross-domain propagation
    - missing parent events
    - duplicates
    - severity changes
    - noisy fault-code names
    - topology distance labels
    """
    scenario = random.choice(FAULT_SCENARIOS)

    storm_id = str(uuid.uuid4())
    root_node_id = choose_node_in_domain(
        scenario["root_domain"]
    )

    parent_missing = (
        random.random() < MISSING_PARENT_PROBABILITY
    )

    parent_alarm = make_alarm(
        storm_id=storm_id,
        event_role="PARENT",
        root_cause_event_id=None,
        root_cause_fault_code=scenario["fault_code"],
        node_id=root_node_id,
        fault_code=scenario["fault_code"],
        severity=scenario["severity"],
        message=scenario["message"],
        delay_seconds=0,
        topology_distance_from_root=0,
        is_ground_truth_anomaly=False,
        parent_missing=parent_missing,
    )

    # The parent event ID is the ground-truth root cause even if the
    # parent alarm is deliberately not sent to Netcool.
    root_event_id = parent_alarm["event_id"]

    parent_alarm["parent_alarm_id"] = root_event_id
    parent_alarm["ground_truth_root_cause_event_id"] = root_event_id

    alarms: list[dict[str, Any]] = []

    # Missing parent alarm:
    # The incident exists, but the root alarm is not emitted.
    if not parent_missing:
        alarms.append(parent_alarm)

    storm_size = random.randint(
        MIN_STORM_SIZE,
        MAX_STORM_SIZE,
    )

    for _ in range(storm_size - 1):
        target_domain = random.choice(
            scenario["propagation_domains"]
        )

        child_node_id = choose_node_in_domain(
            target_domain
        )

        topology_distance = shortest_topology_distance(
            root_node_id,
            child_node_id,
        )

        # Greater topology distance usually creates a larger delay.
        # This mimics propagation from RAN -> Transport -> Core.
        base_delay = topology_distance * random.uniform(
            3,
            10,
        )

        random_delay = random.uniform(
            0,
            MAX_CHILD_DELAY_SECONDS,
        )

        delay_seconds = min(
            base_delay + random_delay,
            MAX_CHILD_DELAY_SECONDS,
        )

        child_fault_code = (
            f"IMPACT_{scenario['fault_code']}"
        )

        # Some alarms have inconsistent vendor/tool naming.
        if random.random() < NOISY_FAULT_CODE_PROBABILITY:
            child_fault_code = mutate_fault_code(
                child_fault_code
            )

        child_severity = change_severity(
            scenario["severity"]
        )

        child_alarm = make_alarm(
            storm_id=storm_id,
            event_role="CHILD",
            root_cause_event_id=root_event_id,
            root_cause_fault_code=scenario["fault_code"],
            node_id=child_node_id,
            fault_code=child_fault_code,
            severity=child_severity,
            message=(
                f"Cross-domain impact from "
                f"{scenario['fault_code']}"
            ),
            delay_seconds=delay_seconds,
            topology_distance_from_root=topology_distance,
            is_ground_truth_anomaly=False,
            parent_missing=parent_missing,
        )

        alarms.append(child_alarm)

        # Duplicate notification event.
        if random.random() < DUPLICATE_PROBABILITY:
            alarms.append(
                create_duplicate_alarm(child_alarm)
            )

    return alarms


def generate_unrelated_background_alarm() -> dict[str, Any]:
    """
    Generate a legitimate but unrelated alarm during another storm.
    """
    scenario = random.choice(FAULT_SCENARIOS)
    node_id = choose_node_in_domain(
        scenario["root_domain"]
    )

    fault_code = scenario["fault_code"]

    if random.random() < NOISY_FAULT_CODE_PROBABILITY:
        fault_code = mutate_fault_code(fault_code)

    return make_alarm(
        storm_id=str(uuid.uuid4()),
        event_role="BACKGROUND",
        root_cause_event_id=None,
        root_cause_fault_code=None,
        node_id=node_id,
        fault_code=fault_code,
        severity=change_severity(scenario["severity"]),
        message="Unrelated background network event",
        delay_seconds=random.uniform(0, 40),
        topology_distance_from_root=None,
        is_ground_truth_anomaly=False,
    )


def generate_true_anomaly() -> dict[str, Any]:
    """
    Generate an event intentionally unlike normal incident patterns.
    """
    node_id = random.choice(
        list(TOPOLOGY_NODES.keys())
    )

    unusual_fault_codes = [
        "UNKNOWN_PROTOCOL_DRIFT",
        "SILENT_PACKET_BLACKHOLE",
        "UNEXPECTED_MULTI_DOMAIN_SPIKE",
        "GHOST_SESSION_SURGE",
        "UNCLASSIFIED_SIGNAL_PATTERN",
    ]

    return make_alarm(
        storm_id=str(uuid.uuid4()),
        event_role="ANOMALY",
        root_cause_event_id=None,
        root_cause_fault_code=None,
        node_id=node_id,
        fault_code=random.choice(unusual_fault_codes),
        severity=random.choice(
            ["WARNING", "MAJOR", "CRITICAL"]
        ),
        message="Synthetic unknown behaviour injected",
        delay_seconds=random.uniform(0, 50),
        topology_distance_from_root=None,
        is_ground_truth_anomaly=True,
    )


def generate_mixed_event_batch() -> list[dict[str, Any]]:
    """
    Build one realistic batch containing:
    - one primary fault storm
    - unrelated alarms in the same time window
    - optional anomaly
    """
    events = generate_strong_fault_storm()

    if random.random() < UNRELATED_ALARM_PROBABILITY:
        background_count = random.randint(
            MIN_BACKGROUND_ALARMS,
            MAX_BACKGROUND_ALARMS,
        )

        for _ in range(background_count):
            events.append(
                generate_unrelated_background_alarm()
            )

    if random.random() < ANOMALY_PROBABILITY:
        events.append(generate_true_anomaly())

    # Sort by simulated timestamp so delayed children appear naturally.
    events.sort(key=lambda event: event["timestamp"])

    return events


# ============================================================
# WebSocket server
# ============================================================

async def register_client(
    websocket: ServerConnection,
) -> None:
    """Register a client that receives the shared stream."""
    connected_clients.add(websocket)

    logging.info(
        "Client connected | total clients=%d",
        len(connected_clients),
    )

    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)

        logging.info(
            "Client disconnected | total clients=%d",
            len(connected_clients),
        )


async def broadcast_alarm(alarm: dict[str, Any]) -> None:
    """Broadcast one alarm event to all connected clients."""
    if not connected_clients:
        return

    payload = json.dumps(alarm)
    disconnected_clients = set()

    for client in connected_clients.copy():
        try:
            await client.send(payload)
        except ConnectionClosed:
            disconnected_clients.add(client)

    connected_clients.difference_update(
        disconnected_clients
    )


async def alarm_producer() -> None:
    """Continuously generate and broadcast realistic mixed alarm batches."""
    logging.info("Strong alarm producer started.")

    while not shutdown_event.is_set():
        batch = generate_mixed_event_batch()

        root_count = sum(
            event["alarm_role"] == "PARENT"
            for event in batch
        )

        child_count = sum(
            event["alarm_role"] == "CHILD"
            for event in batch
        )

        duplicate_count = sum(
            event["ground_truth_is_duplicate"]
            for event in batch
        )

        anomaly_count = sum(
            event["ground_truth_is_anomaly"]
            for event in batch
        )

        logging.info(
            "Generated mixed batch | events=%d | parents=%d | "
            "children=%d | duplicates=%d | anomalies=%d",
            len(batch),
            root_count,
            child_count,
            duplicate_count,
            anomaly_count,
        )

        for alarm in batch:
            if shutdown_event.is_set():
                break

            await broadcast_alarm(alarm)
            await asyncio.sleep(EVENT_INTERVAL_SECONDS)

        await asyncio.sleep(STORM_INTERVAL_SECONDS)


# ============================================================
# Application entry point
# ============================================================

async def main() -> None:
    async with serve(register_client, HOST, PORT):
        logging.info(
            "Strong simulator running at ws://%s:%d",
            HOST,
            PORT,
        )

        producer_task = asyncio.create_task(
            alarm_producer()
        )

        await shutdown_event.wait()

        producer_task.cancel()

        try:
            await producer_task
        except asyncio.CancelledError:
            pass


def request_shutdown() -> None:
    logging.info("Shutdown requested.")
    shutdown_event.set()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        request_shutdown()
    finally:
        loop.close()