# Craig Tomkow
# 2022-10-17

# local imports
import config
import exception

# python imports
from pathlib import Path
import json
import struct
import socket
from typing import List, Tuple
import os
import base64

# 3rd party imports
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def append_name_to_dir_list(name: str, dirs: list) -> List[str]:

    return [s + name for s in dirs]


# all the possible directories for the cfg files,
#   depending on how things are packaged and deployed
#   starts with local directory
#   then a dedicated user etc. folder
#   then branches out eventually system-wide
#
#   if multiple config files are found, the last one read will overwrite the earlier files found
def conf_dir() -> List[str]:

    return [
        './',
        'cfg/',
        f'{Path.home().resolve().as_posix()}/.gluetube/etc/',
        '/usr/local/etc/gluetube/',
        '/etc/opt/gluetube/',
        '/etc/gluetube/'
    ]


def conf() -> config.Gluetube:

    try:
        gt_cfg = config.Gluetube(append_name_to_dir_list('gluetube.cfg', conf_dir()))
        gt_cfg.parse()
        return gt_cfg
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e


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


def encrypt(data: str, sys_password: base64.urlsafe_b64encode) -> Tuple[str, str]:

    salt = os.urandom(16)
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=500000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(sys_password))
    return base64.urlsafe_b64encode(Fernet(key).encrypt(data.encode())).decode(), base64.urlsafe_b64encode(salt).decode()


def decrypt(data: str, sys_password: base64.urlsafe_b64encode, salt: str) -> str:

    salt = base64.urlsafe_b64decode(salt.encode())
    data = base64.urlsafe_b64decode(data.encode())
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=500000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(sys_password))
    return Fernet(key).decrypt(data).decode()
