import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


# ============================================================
# Configuration
# ============================================================

# Input: alarm simulator WebSocket server
SIMULATOR_URI = "ws://localhost:8765"

# Output: Netcool emulator WebSocket server
NETCOOL_HOST = "localhost"
NETCOOL_PORT = 8766

# Correlation window:
# Netcool waits up to 45 seconds to find related child alarms.
CORRELATION_WINDOW_SECONDS = 45

# How often the correlation engine processes buffered alarms.
PROCESSING_INTERVAL_SECONDS = 5

# Keep parent alarms in memory slightly longer than the window.
PARENT_RETENTION_SECONDS = 60

# JSON Lines output file.
OUTPUT_DIRECTORY = Path("output")
OUTPUT_FILE = OUTPUT_DIRECTORY / "netcool_events.jsonl"


# ============================================================
# Logging and global state
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Connected dashboard / downstream clients.
connected_clients: set[ServerConnection] = set()

# Stores incoming raw alarms before they are processed.
incoming_alarm_buffer: deque[dict[str, Any]] = deque()

# Stores active parent/root-cause alarms.
active_parent_alarms: deque[dict[str, Any]] = deque()

# Prevents the same event from being written multiple times.
processed_event_ids: set[str] = set()

shutdown_event = asyncio.Event()


# ============================================================
# Static Netcool correlation rules
# ============================================================

STATIC_CORRELATION_RULES = {
    "RAN_RF_FAILURE": {
        "expected_child_fault_code": "IMPACT_RAN_RF_FAILURE",
        "root_cause_domain": "RAN",
        "description": "RAN radio-frequency failure causes downstream impact alarms.",
    },
    "TRANSPORT_LINK_DOWN": {
        "expected_child_fault_code": "IMPACT_TRANSPORT_LINK_DOWN",
        "root_cause_domain": "TRANSPORT",
        "description": "Transport link failure causes downstream impact alarms.",
    },
    "CORE_SERVICE_DEGRADED": {
        "expected_child_fault_code": "IMPACT_CORE_SERVICE_DEGRADED",
        "root_cause_domain": "CORE",
        "description": "Core degradation causes dependent service alarms.",
    },
    "EPC_SIGNALING_FAILURE": {
        "expected_child_fault_code": "IMPACT_EPC_SIGNALING_FAILURE",
        "root_cause_domain": "EPC",
        "description": "EPC signaling failure causes service-impact alarms.",
    },
    "HIGH_PACKET_LOSS": {
        "expected_child_fault_code": "IMPACT_HIGH_PACKET_LOSS",
        "root_cause_domain": "TRANSPORT",
        "description": "High packet loss causes service degradation alarms.",
    },
    "CELL_CONGESTION": {
        "expected_child_fault_code": "IMPACT_CELL_CONGESTION",
        "root_cause_domain": "RAN",
        "description": "Cell congestion causes user-service impact alarms.",
    },
}


# ============================================================
# Utility functions
# ============================================================

def parse_timestamp(timestamp: str) -> datetime:
    """
    Convert ISO-8601 timestamp from the simulator into datetime.
    """
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


def seconds_between(first: dict[str, Any], second: dict[str, Any]) -> float:
    """
    Return absolute time difference between two alarm events.
    """
    first_time = parse_timestamp(first["timestamp"])
    second_time = parse_timestamp(second["timestamp"])

    return abs((second_time - first_time).total_seconds())


def ensure_output_directory() -> None:
    """
    Create output directory if it does not already exist.
    """
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)


def write_json_event(event: dict[str, Any]) -> None:
    """
    Append one event as one line of JSON.

    JSON Lines format is ideal for:
    - pandas
    - Spark
    - scikit-learn preprocessing
    - streaming pipelines
    """
    ensure_output_directory()

    with OUTPUT_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event) + "\n")


# ============================================================
# Output WebSocket server
# ============================================================

async def dashboard_client_handler(websocket: ServerConnection) -> None:
    """
    Accept downstream clients such as:
    - Streamlit dashboard
    - AI correlation engine
    - test client
    """
    connected_clients.add(websocket)

    logging.info(
        "Downstream client connected. Total downstream clients: %d",
        len(connected_clients),
    )

    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)

        logging.info(
            "Downstream client disconnected. Total downstream clients: %d",
            len(connected_clients),
        )


async def broadcast_correlated_event(event: dict[str, Any]) -> None:
    """
    Send correlated events to all connected downstream clients.
    """
    if not connected_clients:
        return

    payload = json.dumps(event)
    disconnected_clients = set()

    for client in connected_clients.copy():
        try:
            await client.send(payload)
        except ConnectionClosed:
            disconnected_clients.add(client)

    connected_clients.difference_update(disconnected_clients)


# ============================================================
# Input WebSocket client
# ============================================================

async def receive_simulator_alarms() -> None:
    """
    Connect to alarm_simulator.py and receive raw JSON alarms.
    Automatically reconnect if the simulator restarts.
    """
    while not shutdown_event.is_set():
        try:
            logging.info("Connecting to simulator: %s", SIMULATOR_URI)

            async with connect(SIMULATOR_URI) as websocket:
                logging.info("Connected to alarm simulator.")

                async for message in websocket:
                    raw_alarm = json.loads(message)

                    incoming_alarm_buffer.append(raw_alarm)

                    logging.info(
                        "Received raw alarm | event_id=%s | role=%s | fault=%s",
                        raw_alarm["event_id"],
                        raw_alarm["alarm_role"],
                        raw_alarm["fault_code"],
                    )

        except ConnectionRefusedError:
            logging.warning(
                "Simulator is not running. Retrying in 3 seconds..."
            )
            await asyncio.sleep(3)

        except ConnectionClosed:
            logging.warning(
                "Simulator connection closed. Retrying in 3 seconds..."
            )
            await asyncio.sleep(3)

        except Exception as error:
            logging.exception(
                "Unexpected simulator connection error: %s",
                error,
            )
            await asyncio.sleep(3)


# ============================================================
# Netcool rule engine
# ============================================================

def is_parent_alarm(alarm: dict[str, Any]) -> bool:
    """
    A parent/root-cause alarm must:
    1. Have role PARENT
    2. Have a fault code defined in static rules
    """
    return (
        alarm.get("alarm_role") == "PARENT"
        and alarm.get("fault_code") in STATIC_CORRELATION_RULES
    )


def find_matching_parent(
    child_alarm: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Find a parent alarm that matches the static Netcool rule.

    A match requires:
    - Parent fault code has a defined rule
    - Child fault code matches expected child fault code
    - Child arrives within the 45-second correlation window
    """
    for parent_alarm in reversed(active_parent_alarms):
        parent_fault_code = parent_alarm["fault_code"]
        rule = STATIC_CORRELATION_RULES.get(parent_fault_code)

        if rule is None:
            continue

        expected_child_fault = rule["expected_child_fault_code"]

        if child_alarm["fault_code"] != expected_child_fault:
            continue

        time_difference = seconds_between(parent_alarm, child_alarm)

        if time_difference <= CORRELATION_WINDOW_SECONDS:
            return parent_alarm

    return None


def build_netcool_event(
    raw_alarm: dict[str, Any],
    matched_parent: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Add Netcool-style correlation fields to a raw alarm.
    """
    event = raw_alarm.copy()

    event["netcool_processed_at"] = datetime.now(
        timezone.utc
    ).isoformat()

    if matched_parent is not None:
        event["netcool_status"] = "CORRELATED_CHILD"
        event["netcool_root_cause_event_id"] = matched_parent["event_id"]
        event["netcool_root_cause_fault_code"] = matched_parent["fault_code"]
        event["netcool_correlation_method"] = "STATIC_RULE"
        event["netcool_correlation_window_seconds"] = (
            CORRELATION_WINDOW_SECONDS
        )

    elif is_parent_alarm(raw_alarm):
        rule = STATIC_CORRELATION_RULES[raw_alarm["fault_code"]]

        event["netcool_status"] = "ROOT_CAUSE"
        event["netcool_root_cause_event_id"] = raw_alarm["event_id"]
        event["netcool_root_cause_fault_code"] = raw_alarm["fault_code"]
        event["netcool_correlation_method"] = "STATIC_RULE"
        event["netcool_rule_description"] = rule["description"]

    else:
        event["netcool_status"] = "UNMATCHED"
        event["netcool_root_cause_event_id"] = None
        event["netcool_root_cause_fault_code"] = None
        event["netcool_correlation_method"] = "NO_MATCH"

    return event


def remove_expired_parents() -> None:
    """
    Remove parent alarms older than the retention period.
    """
    now = datetime.now(timezone.utc)

    while active_parent_alarms:
        oldest_parent = active_parent_alarms[0]
        parent_time = parse_timestamp(oldest_parent["timestamp"])

        age_seconds = (now - parent_time).total_seconds()

        if age_seconds > PARENT_RETENTION_SECONDS:
            removed_parent = active_parent_alarms.popleft()

            logging.info(
                "Removed expired parent alarm: %s",
                removed_parent["event_id"],
            )
        else:
            break


async def process_alarm_buffer() -> None:
    """
    Every 5 seconds:
    - process received alarms
    - apply static rules
    - write JSON output
    - broadcast correlated result
    """
    while not shutdown_event.is_set():
        await asyncio.sleep(PROCESSING_INTERVAL_SECONDS)

        remove_expired_parents()

        if not incoming_alarm_buffer:
            continue

        logging.info(
            "Processing buffered alarms: %d",
            len(incoming_alarm_buffer),
        )

        alarms_to_process = []

        while incoming_alarm_buffer:
            alarms_to_process.append(incoming_alarm_buffer.popleft())

        # Process parent alarms first.
        # This ensures children can find their root cause.
        alarms_to_process.sort(
            key=lambda alarm: 0 if alarm["alarm_role"] == "PARENT" else 1
        )

        for raw_alarm in alarms_to_process:
            event_id = raw_alarm["event_id"]

            if event_id in processed_event_ids:
                continue

            matched_parent = None

            if is_parent_alarm(raw_alarm):
                active_parent_alarms.append(raw_alarm)

            elif raw_alarm["alarm_role"] == "CHILD":
                matched_parent = find_matching_parent(raw_alarm)

            netcool_event = build_netcool_event(
                raw_alarm=raw_alarm,
                matched_parent=matched_parent,
            )

            write_json_event(netcool_event)
            await broadcast_correlated_event(netcool_event)

            processed_event_ids.add(event_id)

            logging.info(
                "Netcool processed | event_id=%s | status=%s | root_cause=%s",
                netcool_event["event_id"],
                netcool_event["netcool_status"],
                netcool_event["netcool_root_cause_fault_code"],
            )


# ============================================================
# Shutdown handling
# ============================================================

def request_shutdown() -> None:
    logging.info("Shutdown requested.")
    shutdown_event.set()


# ============================================================
# Main application
# ============================================================

async def main() -> None:
    ensure_output_directory()

    logging.info(
        "Starting Netcool emulator output server at ws://%s:%d",
        NETCOOL_HOST,
        NETCOOL_PORT,
    )

    async with serve(
        dashboard_client_handler,
        NETCOOL_HOST,
        NETCOOL_PORT,
    ):
        logging.info(
            "Netcool output WebSocket available at ws://%s:%d",
            NETCOOL_HOST,
            NETCOOL_PORT,
        )

        receiver_task = asyncio.create_task(
            receive_simulator_alarms()
        )

        processor_task = asyncio.create_task(
            process_alarm_buffer()
        )

        await shutdown_event.wait()

        receiver_task.cancel()
        processor_task.cancel()

        try:
            await receiver_task
        except asyncio.CancelledError:
            pass

        try:
            await processor_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        request_shutdown()