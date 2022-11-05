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
import struct

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
    table.field_names = [
        'pipeline name', 'schedule ID', 'cron', 'run at (IS0 8601)', 'paused', 'status', 'stage message', 'end time (ISO 8601)'
    ]
    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    details = db.ls_pipelines()
    table.add_rows(details)
    return table


# TODO: change this to making an RPC call to the daemon to trigger the scheduler to run pipeline once, immediately
def pipeline_run(name: str) -> None:

    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    pipeline_id = db.pipeline_id_from_name(name)
    pipeline_py = db.pipeline_py_from_name(name)
    pipeline_dir = db.pipeline_dir_from_name(name)
    # TODO: also need to inject custom gluetube env vars into instance
    try:
        runner = Runner(pipeline_id, name, pipeline_py, pipeline_dir)
    except exceptions.RunnerError:
        raise

    runner.run()


def gluetube_dev(msg: str) -> None:

    msg_bytes = str.encode(msg)
    msg = struct.pack('>I', len(msg_bytes)) + msg_bytes
    try:
        util.send_rpc_msg_to_daemon(msg)
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


def schedule_cron(schedule_id: int, cron: str) -> None:

    # TODO: basic cron validation

    msg = util.craft_rpc_msg('set_schedule_cron', [schedule_id, cron])

    try:
        util.send_rpc_msg_to_daemon(msg)
    except exceptions.rpcError:
        raise


def schedule_at(schedule_id: int, run_date_time: str) -> None:

    # TODO: validate run_date_time is valid ISO 8601 string

    msg = util.craft_rpc_msg('set_schedule_at', [schedule_id, run_date_time])

    try:
        util.send_rpc_msg_to_daemon(msg)
    except exceptions.rpcError:
        raise


def schedule_new(pipeline_name: str) -> None:

    try:
        db = Pipeline('gluetube.db')
    except exceptions.dbError:
        raise

    pipeline_id = db.pipeline_id_from_name(pipeline_name)

    msg = util.craft_rpc_msg('set_schedule_new', [pipeline_id])

    try:
        util.send_rpc_msg_to_daemon(msg)
    except exceptions.rpcError:
        raise
