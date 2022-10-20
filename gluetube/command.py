# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import config
import util
from gluetubed import GTdaemon
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
    db = Pipeline('gluetube.db')
    db.create_schema()
    db = Store('store.db')
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


def start_daemon() -> None:

    GTdaemon().start()


def start_daemon_fg() -> None:

    GTdaemon().start(fg=True)


# TODO: extract the message passing to it's own helper method
def pipeline_update_cron(name: str, cron: str) -> None:

    msg = {'function': '_update_schedule_cron', 'parameters': [name, cron]}
    msg = json.dumps(msg)
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
