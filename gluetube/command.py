# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import util
from gluetubed import GluetubeDaemon
from runner import Runner
import exception

# python imports
from pathlib import Path
import struct

# 3rd party imports
from prettytable import PrettyTable
from prettytable import SINGLE_BORDER


def gluetube_init() -> None:

    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    Path(gt_cfg.pipeline_dir).mkdir(parents=True, exist_ok=True)
    Path(gt_cfg.sqlite_dir).mkdir(parents=True, exist_ok=True)
    Path(gt_cfg.runner_tmp_dir).mkdir(parents=True, exist_ok=True)

    try:
        db = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name), read_only=False)
    except exception.dbError:
        raise

    db.create_schema()

    try:
        db = Store(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_kv_name), read_only=False)
        db.create_table('common')
    except exception.dbError:
        raise

    print('setup complete.')


def summary() -> PrettyTable:

    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    table = PrettyTable()
    table.set_style(SINGLE_BORDER)
    table.field_names = [
        'pipeline name', 'file name', 'schedule ID', 'cron', 'run at (IS0 8601)', 'paused', 'status', 'stage message', 'end time (ISO 8601)'
    ]

    try:
        db = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name))
    except exception.dbError:
        raise

    details = db.summary_pipelines()
    table.add_rows(details)
    return table


def pipeline_run(name: str) -> None:

    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    try:
        db = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name))
    except exception.dbError:
        raise

    pipeline_id = db.pipeline_id_from_name(name)
    pipeline_py = db.pipeline_py_from_name(name)
    pipeline_dir = db.pipeline_dir_from_name(name)
    # TODO: also need to inject custom gluetube env vars into instance
    try:
        runner = Runner(pipeline_id, name, pipeline_py, pipeline_dir, 0)  # TODO: handle manual run with no schedule
    except exception.RunnerError:
        raise

    runner.run()
    TODO: work in progress
    # msg = util.craft_rpc_msg('set_pipeline', [key])

    # try:
    #     util.send_rpc_msg_to_daemon(msg, socket_file)
    # except exception.rpcError:
    #     raise


def pipeline_schedule(pipeline_name: str, socket_file: Path) -> None:
    # TODO: remove this config and pass in vars instead
    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    try:
        db = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name))
    except exception.dbError:
        raise

    pipeline_id = db.pipeline_id_from_name(pipeline_name)

    msg = util.craft_rpc_msg('set_schedule', [pipeline_id])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def gluetube_dev(msg: str, socket_file: Path) -> None:

    msg_bytes = str.encode(msg)
    msg = struct.pack('>I', len(msg_bytes)) + msg_bytes
    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def daemon_fg(debug: bool) -> None:

    try:
        GluetubeDaemon().start(debug, fg=True)
    except exception.DaemonError:
        raise


def daemon_bg(debug: bool) -> None:

    try:
        GluetubeDaemon().start(debug)
    except exception.DaemonError:
        raise


def schedule_cron(schedule_id: int, cron: str, socket_file: Path) -> None:

    # TODO: basic cron validation

    msg = util.craft_rpc_msg('set_schedule_cron', [schedule_id, cron])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def schedule_at(schedule_id: int, at: str, socket_file: Path) -> None:

    # TODO: validate run_date_time is valid ISO 8601 string

    msg = util.craft_rpc_msg('set_schedule_at', [schedule_id, at])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def store_add(key: str, value: str, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('set_key_value', [key, value])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def store_delete(key: str, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('delete_key', [key])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def store_ls() -> PrettyTable:

    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    table = PrettyTable()
    table.set_style(SINGLE_BORDER)
    table.field_names = ['keys']

    try:
        db = Store(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_kv_name))
    except exception.dbError:
        raise

    details = db.all_keys('common')
    table.add_rows(details)
    return table
