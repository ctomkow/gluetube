# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import config
import util
from gluetubed import GluetubeDaemon
from runner import Runner

# python imports
from pathlib import Path
import socket
import struct
import json


def init_gluetube() -> None:

    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    Path(gt_cfg.pipeline_dir).mkdir(exist_ok=True)
    Path(gt_cfg.database_dir).mkdir(exist_ok=True)
    db = Pipeline('gluetube.db', read_only=False)
    db.create_schema()
    db = Store('store.db', read_only=False)
    print('setup complete.')


def ls_pipelines() -> list:

    db = Pipeline('gluetube.db')
    return db.all_pipelines()


def run_pipeline(name: str) -> None:
    db = Pipeline('gluetube.db')
    pipeline_py = db.pipeline_py_name(name)
    pipeline_dir = db.pipeline_dir_name(name)
    Runner(name, pipeline_py, pipeline_dir).run()


# TODO: extract the message passing to it's own helper method
def dev_msg_to_daemon(msg: str) -> None:

    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    server_address = gt_cfg.socket_file
    if not Path(gt_cfg.socket_file).exists():
        raise FileNotFoundError(F"Unix domain socket, {gt_cfg.socket_file}, not found")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(server_address)
    msg = struct.pack('>I', len(msg)) + str.encode(msg)
    try:
        sock.sendall(msg)
    except ConnectionRefusedError:
        raise


def start_daemon_bg() -> None:

    GluetubeDaemon().start()


def start_daemon_fg() -> None:

    GluetubeDaemon().start(fg=True)


# TODO: extract the message passing to it's own helper method
def pipeline_set_cron(name: str, cron: str) -> None:

    msg_dict = {'function': 'set_cron', 'parameters': [name, cron]}
    msg_str = json.dumps(msg_dict)
    msg_bytes = str.encode(msg_str)
    msg_packet = struct.pack('>I', len(msg_bytes)) + msg_bytes

    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    server_address = gt_cfg.socket_file
    if not Path(gt_cfg.socket_file).exists():
        raise FileNotFoundError(F"Unix domain socket, {gt_cfg.socket_file}, not found")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(server_address)
        sock.sendall(msg_packet)
    except ConnectionRefusedError:
        raise
