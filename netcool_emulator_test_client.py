import asyncio
import json

from websockets.asyncio.client import connect


async def receive_netcool_events() -> None:
    uri = "ws://localhost:8766"

    async with connect(uri) as websocket:
        print(f"Connected to Netcool emulator: {uri}")

        async for message in websocket:
            event = json.loads(message)

            print(
                f"{event['netcool_status']:18} | "
                f"{event['fault_code']:35} | "
                f"root cause: {event['netcool_root_cause_fault_code']}"
            )


if __name__ == "__main__":
    asyncio.run(receive_netcool_events())