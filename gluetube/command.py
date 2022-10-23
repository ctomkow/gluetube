# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import config
import util
from gluetubed import GluetubeDaemon
from runner import Runner
import exceptions
from autodiscovery import PipelineScanner

# python imports
from pathlib import Path
import socket
import struct
import json

# 3rd party imports
from prettytable import PrettyTable
from prettytable import SINGLE_BORDER


def gluetube_init() -> None:

    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    Path(gt_cfg.pipeline_dir).mkdir(parents=True, exist_ok=True)
    Path(gt_cfg.database_dir).mkdir(parents=True, exist_ok=True)
    db = Pipeline('gluetube.db', read_only=False)
    db.create_schema()
    db = Store('store.db', read_only=False)
    print('setup complete.')


def gluetube_ls() -> list:

    table = PrettyTable()
    table.set_style(SINGLE_BORDER)
    table.field_names = ['id', 'pipeline name', 'py file', 'directory name', 'cron schedule']
    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    details = db.all_pipelines_details()
    table.add_rows(details)
    return table


# TODO: change this to making an RPC call to the daemon to trigger the scheduler to run pipeline once, immediately
def pipeline_run(name: str) -> None:

    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    pipeline_py = db.pipeline_py_name(name)
    pipeline_dir = db.pipeline_dir_name(name)

    try:
        runner = Runner(name, pipeline_py, pipeline_dir)
    except exceptions.RunnerError:
        raise

    runner.run()


def gluetube_dev(msg: str) -> None:

    msg_bytes = str.encode(msg)
    msg = struct.pack('>I', len(msg_bytes)) + msg_bytes
    try:
        _send_rpc_msg_to_daemon(msg)
    except exceptions.rpcError:
        raise


def gluetube_scan() -> None:

    # TODO: try/except
    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    PipelineScanner(gt_cfg.pipeline_dir).scan()


def daemon_fg(debug: bool) -> None:

    try:
        GluetubeDaemon().start(debug, fg=True)
    except exceptions.DaemonError:
        raise


def daemon_bg(debug: bool) -> None:

    try:
        GluetubeDaemon().start(debug)
    except exceptions.DaemonError:
        raise


def pipeline_cron(name: str, cron: str) -> None:

    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    pipeline_id = db.pipeline_id(name)

    msg = _craft_rpc_msg('set_cron', [pipeline_id, cron])

    try:
        _send_rpc_msg_to_daemon(msg)
    except exceptions.rpcError:
        raise

# helper functions


def _craft_rpc_msg(func: str, params: list) -> bytes:

    msg_dict = {'function': func, 'parameters': params}
    msg_str = json.dumps(msg_dict)
    msg_bytes = str.encode(msg_str)
    return struct.pack('>I', len(msg_bytes)) + msg_bytes


def _send_rpc_msg_to_daemon(msg: bytes) -> None:

    try:
        gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
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
