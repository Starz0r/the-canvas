import asyncio
import base64
import http
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Final, Union

import ujson
from websockets import Headers, WebSocketServer, WebSocketServerProtocol, broadcast


@dataclass
class Placement:
    cmd: str
    _type: int = -1
    eid: str = "0"
    x: float = 0.0
    y: float = 0.0
    created_by: int = -1
    date_created: int = -1
    removed: bool = False
    date_removed: int = -1


@dataclass
class PlacementRemoved:
    cmd: str
    eid: str = ""


@dataclass
class ClientVersion:
    cmd: str
    version: int = 1000


@dataclass
class Session:
    remote_addr: str = "0.0.0.0"
    actions: int = 1


EVLOOP: Final[asyncio.AbstractEventLoop] = asyncio.new_event_loop()
RUNNING: Final[asyncio.Future]
WS_SERV: Final[WebSocketServer]
OBJ_LIST: Dict[str, Placement] = {}
SESSION_LIST: Dict[str, Session] = {}
REQUIRED_CLIENT_VERSION: Final[int] = 1000


async def extract_struct_from_msg(
    payload: Dict[Any, Any],
) -> Union[Placement, ClientVersion, PlacementRemoved]:
    cmd = payload.get("cmd")
    if not cmd:
        # TODO: log here
        return
    if cmd == "newplacement":
        return Placement(**payload)
    elif cmd == "clientversion":
        return ClientVersion(**payload)
    elif cmd == "rmplacement":
        return PlacementRemoved(**payload)


async def handle_user_commands(ws: WebSocketServerProtocol):
    # get canonical address
    remote_addr = ws.remote_address[0]
    if type(remote_addr) is not str:
        if remote_addr is None:
            remote_addr = "0.0.0.0"
        else:
            remote_addr = str(remote_addr)

    forwarded_addr = ws.request_headers.get("X-Forwarded-For")
    if remote_addr != forwarded_addr and forwarded_addr is not None:
        remote_addr = forwarded_addr

    # sync client up to date
    for placement in OBJ_LIST.values():
        await ws.send(ujson.dumps({"cmd": "newplacement", **asdict(placement)}))

    # get or create session
    session = SESSION_LIST.pop(remote_addr, None)
    if session is None:
        session = Session(remote_addr, 1)
    SESSION_LIST.update([(remote_addr, session)])

    last_msg_time = time.time()
    deficit_time = 0.0

    # handle commands
    async for msg in ws:
        session = SESSION_LIST.pop(remote_addr)

        # calculate action bank
        now_time = time.time()
        deficit_time += now_time - last_msg_time
        last_msg_time = time.time()

        while deficit_time >= 5:
            session.actions += 1
            if session.actions >= 5:
                deficit_time = 0
            deficit_time -= 5

        if type(msg) is bytes:
            # TODO: log here
            return

        payload = ujson.loads(msg)
        struct = await extract_struct_from_msg(payload)

        if isinstance(struct, Placement):
            if session.actions < 1:
                SESSION_LIST.update([(remote_addr, session)])
                continue
            session.actions -= 1
            # TODO: check if there is a placement at the position already
            # TODO: drop the removed, created_by, date_created field if defined
            struct.cmd = "newplacement"
            struct.eid = base64.urlsafe_b64encode(os.urandom(16)).decode("utf-8")
            struct.date_created = round(time.time() * 1000)
            # TODO: created_by

            OBJ_LIST.update([(struct.eid, struct)])

            print(ujson.dumps({"cmd": "placementaccepted", **asdict(struct)}))

            await ws.send(ujson.dumps({"cmd": "placementaccepted"}))
            broadcast(
                WS_SERV.websockets,
                ujson.dumps({"cmd": "newplacement", **asdict(struct)}),
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
        elif isinstance(struct, PlacementRemoved):
            if session.actions < 1:
                SESSION_LIST.update([(remote_addr, session)])
                continue
            struct.cmd = "rmplacement"
            OBJ_LIST.pop(struct.eid)

            await ws.send(ujson.dumps({"cmd": "placementaccepted"}))
            broadcast(
                WS_SERV.websockets,
                ujson.dumps({"cmd": "rmplacement", **asdict(struct)}),
            )

        SESSION_LIST.update([(remote_addr, session)])

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
