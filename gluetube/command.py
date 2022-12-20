# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import util
from gluetubed import GluetubeDaemon
import exception

# python imports
from pathlib import Path
import struct
import os
import signal
import shutil

# 3rd party imports
from prettytable import PrettyTable
from prettytable import SINGLE_BORDER
from cryptography.fernet import Fernet


# this should be idempotent
def gluetube_initdb() -> None:

    try:
        gt_cfg = util.conf()
    except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
        raise e

    # pipeline.db setup
    try:
        db = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name), read_only=False)
    except exception.dbError:
        raise
    db.create_schema()

    # store.db setup
    store_path = Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_kv_name)

    # generate key and create db
    if not store_path.exists():
        token = Fernet.generate_key()
        gt_cfg.config.set('gluetube', 'SQLITE_TOKEN', token.decode())
        gt_cfg.write()
        try:
            db = Store(token.decode(), db_path=store_path, read_only=False)
            db.create_table('common')
        except exception.dbError:
            raise

    print('database setup complete.')


# this should be idempotent
def gluetube_configure() -> None:

    appdir = Path(Path.home() / '.gluetube')
    appdir.mkdir(parents=True, exist_ok=True)
    Path(appdir, 'pipelines').mkdir(parents=True, exist_ok=True)
    Path(appdir, 'db').mkdir(parents=True, exist_ok=True)
    Path(appdir, 'var').mkdir(parents=True, exist_ok=True)
    Path(appdir, 'etc').mkdir(parents=True, exist_ok=True)
    incl_cfg_location = Path(Path(__file__).parent.resolve() / 'cfg' / 'gluetube.cfg')
    depl_cfg_location = Path(appdir / 'etc' / 'gluetube.cfg')
    if not depl_cfg_location.exists():
        shutil.copy(incl_cfg_location, depl_cfg_location)

    print('first time setup complete')


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


def pipeline_schedule(pipeline_name: str, socket_file: Path) -> None:

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


def daemon_stop(debug: bool) -> None:

    with open('/tmp/gluetube.pid', 'r', encoding="utf-8") as f:
        pid = f.readline()

    os.kill(int(pid), signal.SIGTERM)


def schedule_cron(schedule_id: int, cron: str, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('set_schedule_cron', [schedule_id, cron])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def schedule_at(schedule_id: int, at: str, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('set_schedule_at', [schedule_id, at])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def schedule_now(schedule_id: int, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('set_schedule_now', [schedule_id])

    try:
        util.send_rpc_msg_to_daemon(msg, socket_file)
    except exception.rpcError:
        raise


def schedule_delete(schedule_id: int, socket_file: Path) -> None:

    msg = util.craft_rpc_msg('delete_schedule', [schedule_id])

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
        db = Store(gt_cfg.sqlite_token, db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_kv_name))
    except exception.dbError:
        raise

    details = db.all_keys('common')
    table.add_rows(details)
    return table
