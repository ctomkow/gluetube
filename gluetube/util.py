# Craig Tomkow
# 2022-10-17

# local imports
import config
import exceptions

# python imports
from pathlib import Path
import json
import struct
import socket


def append_name_to_dir_list(name: str, conf_dir: list) -> list:

    return [s + name for s in conf_dir]


# all the possible directories for the cfg files,
#   depending on how things are packaged and deployed
#   starts locally, then branches out eventually system-wide
def conf_dir() -> list:

    return [
        './',
        'cfg/',
        f'{Path.home()}/.gluetube/etc/',
        '/usr/local/etc/gluetube/',
        '/etc/opt/gluetube/',
        '/etc/gluetube/'
    ]


def craft_rpc_msg(func: str, params: list) -> bytes:

    msg_dict = {'function': func, 'parameters': params}
    msg_str = json.dumps(msg_dict)
    msg_bytes = str.encode(msg_str)
    return struct.pack('>I', len(msg_bytes)) + msg_bytes


def send_rpc_msg_to_daemon(msg: bytes) -> None:

    try:
        gt_cfg = config.Gluetube(append_name_to_dir_list('gluetube.cfg', conf_dir()))
    except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
        raise exceptions.rpcError(f"RPC call failed. {e}") from e

    server_address = gt_cfg.socket_file
    if not Path(gt_cfg.socket_file).exists():
        raise exceptions.rpcError(f"Unix domain socket, {gt_cfg.socket_file}, not found")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(server_address)
        sock.sendall(msg)
    except ConnectionRefusedError as e:
        raise exceptions.rpcError(f"RPC call failed. {e}") from e
