import asyncio
import json

from websockets.asyncio.client import connect


async def receive_alarms() -> None:
    uri = "ws://localhost:8765"

    async with connect(uri) as websocket:
        print(f"Connected to {uri}")

        async for message in websocket:
            alarm = json.loads(message)

            print(
                f"[{alarm['timestamp']}] "
                f"{alarm['alarm_role']:7} | "
                f"{alarm['domain']:9} | "
                f"{alarm['severity']:8} | "
                f"{alarm['fault_code']} | "
                f"{alarm['node_id']}"
            )


if __name__ == "__main__":
    asyncio.run(receive_alarms())