import asyncio
import http
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Final, List, Union

import ujson
from websockets import Headers, WebSocketServer, WebSocketServerProtocol


@dataclass
class Placement:
    cmd: str
    _type: int = -1
    eid: int = 0
    x: float = 0.0
    y: float = 0.0
    created_by: int = -1
    date_created: int = -1
    removed: bool = False
    date_removed: int = -1


@dataclass
class ClientVersion:
    cmd: str
    version: int = 1000


EVLOOP: Final[asyncio.AbstractEventLoop] = asyncio.new_event_loop()
RUNNING: Final[asyncio.Future]
WS_SERV: Final[WebSocketServer]
OBJ_LIST: List[Placement] = []
REQUIRED_CLIENT_VERSION: Final[int] = 1000


async def extract_struct_from_msg(
    payload: Dict[Any, Any],
) -> Union[Placement, ClientVersion]:
    cmd = payload.get("cmd")
    if not cmd:
        # TODO: log here
        return
    if cmd == "newplacement":
        return Placement(**payload)
    elif cmd == "clientversion":
        return ClientVersion(**payload)


async def handle_user_commands(ws: WebSocketServerProtocol):
    async for msg in ws:
        if type(msg) is bytes:
            # TODO: log here
            return

        payload = ujson.loads(msg)
        struct = await extract_struct_from_msg(payload)

        if isinstance(struct, Placement):
            # TODO: eat action count from user or drop if they have none
            # TODO: check if there is a placement at the position already
            # TODO: drop the removed, created_by, date_created field if defined
            struct.cmd = "newplacement"
            struct.eid = int.from_bytes(os.urandom(8), byteorder="little", signed=False)
            struct.date_created = round(time.time() * 1000)
            # TODO: created_by

            OBJ_LIST.append(struct)

            print(ujson.dumps({"cmd": "placementaccepted", **asdict(struct)}))

            await ws.send(ujson.dumps({"cmd": "placementaccepted"}))
            await websockets.broadcast(
                WS_SERV.sockets, {"cmd": "newplacement", **asdict(struct)}
            )
        elif isinstance(struct, ClientVersion):
            if not struct.version == REQUIRED_CLIENT_VERSION:
                await ws.send(
                    ujson.dumps(
                        {
                            "cmd": "connectionclosed",
                            "reason": "Client is outdated and does not meet the minimum requirements, please refresh!",
                        }
                    )
                )
                await ws.close()

    # RUNNING.set_result(None)


async def health_check(path: str, request_headers: Headers):
    if path == "/healthz":
        return http.HTTPStatus.OK, [], b"OK\n"


async def start_echo_server():
    from websockets import serve

    global RUNNING, WS_SERV
    RUNNING = asyncio.Future()
    async with serve(
        handle_user_commands, "0.0.0.0", 8765, process_request=health_check
    ) as WS_SERV:
        print("The Canvas is running.")
        await RUNNING


async def run_forever(corofn):
    while True:
        await corofn()


async def main():
    server_task = asyncio.create_task(start_echo_server())
    await asyncio.gather(server_task)


if __name__ == "__main__":
    sys.exit(EVLOOP.run_until_complete(main()))
