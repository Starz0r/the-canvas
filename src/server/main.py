import asyncio
import base64
import http
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Final, List, Optional, Set, Union

import aiofiles
import ujson
from minio import Minio
from minio.datatypes import Object
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
    removed: int = 0


EVLOOP: Final[asyncio.AbstractEventLoop] = asyncio.new_event_loop()
RUNNING: Final[asyncio.Future]
WS_SERV: Final[WebSocketServer]
OBJ_LIST: Dict[str, Placement] = {}
SESSION_LIST: Dict[str, Session] = {}
CONNECTED_REMOTES: Set[str] = set()
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

    if remote_addr in CONNECTED_REMOTES:
        await ws.send(
            ujson.dumps(
                {
                    "cmd": "connectionclosed",
                    "reason": "Session ticket did not match",
                }
            )
        )
        await ws.close()
    CONNECTED_REMOTES.add(remote_addr)

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
    try:
        async for msg in ws:
            session = SESSION_LIST.pop(remote_addr)

            # calculate action bank
            now_time = time.time()
            deficit_time += now_time - last_msg_time
            last_msg_time = time.time()

            while deficit_time >= 15:
                session.actions += 1
                if session.actions >= 15:
                    deficit_time = 0
                deficit_time -= 15

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
    except Exception as e:
        print("An error occured on a socket")
        print(e)
    finally:
        CONNECTED_REMOTES.discard(remote_addr)
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


async def perform_backup():
    while True:
        client = Minio(
            endpoint="sfo3.digitaloceanspaces.com",
            region="sfo3",
            access_key=os.getenv("S3_ACCESS_KEY"),
            secret_key=os.getenv("S3_SECRET_KEY"),
            secure=True,
        )

        epoch = round(time.time() * 1000)
        fname: str
        async with aiofiles.open(
            file=f"{epoch}.json", mode="w+", encoding="utf-8"
        ) as f:
            tmp_list: List[Placement] = []
            for item in OBJ_LIST.values():
                tmp_list.append(asdict(item))
            await f.write(ujson.dumps(tmp_list))
            await f.flush()
            fname = f.name
        client.fput_object("the-canvas", f"{epoch}.json", fname)

        os.remove(fname)

        await asyncio.sleep(600)


async def restore_from_previous_backup():
    client = Minio(
        endpoint="sfo3.digitaloceanspaces.com",
        region="sfo3",
        access_key=os.getenv("S3_ACCESS_KEY"),
        secret_key=os.getenv("S3_SECRET_KEY"),
        secure=True,
    )

    objs = client.list_objects("the-canvas")
    latest: Optional[Object] = None
    for obj in objs:
        if latest is None:
            latest = obj
        else:
            if latest.last_modified < obj.last_modified:
                latest = obj
    client.fget_object("the-canvas", latest.object_name, "latest.json")

    async with aiofiles.open(file="latest.json", mode="r", encoding="utf-8") as f:
        json = await f.read()
        placements: List[Dict[str, Any]] = ujson.loads(json)
        for placement in placements:
            OBJ_LIST.update([(placement["eid"], Placement(**placement))])

    os.remove("latest.json")


async def main():
    await asyncio.create_task(restore_from_previous_backup())

    server_task = asyncio.create_task(start_echo_server())
    backup_task = asyncio.create_task(perform_backup())
    await asyncio.gather(server_task, backup_task)


if __name__ == "__main__":
    sys.exit(EVLOOP.run_until_complete(main()))
