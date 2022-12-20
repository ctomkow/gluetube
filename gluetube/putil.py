# Craig Tomkow
# 2022-12-20
#
# It is important that this module only imports python std libraries!
# As this module is imported by pipeline.py and consequently, by pipelines themselves (within their venv)
# It is important not to have any dependencies that are not present within the pipeline venv itself
# So restrict this module to only python standard libraries

# local imports
import exception

# python imports
from pathlib import Path
import json
import struct
import socket


def craft_rpc_msg(func: str, params: list) -> bytes:

    msg_dict = {'func': func, 'params': params}
    msg_str = json.dumps(msg_dict)
    msg_bytes = str.encode(msg_str)
    return struct.pack('>I', len(msg_bytes)) + msg_bytes


def send_rpc_msg_to_daemon(msg: bytes, socket_file: Path) -> None:

    server_address = socket_file.resolve().as_posix()
    if not socket_file.exists():
        raise exception.rpcError(f"Unix domain socket, {socket_file.resolve().as_posix()}, not found")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(server_address)
        sock.sendall(msg)
    except ConnectionRefusedError as e:
        raise exception.rpcError(f"RPC call failed. {e}") from e
