# Craig Tomkow
# 2022-10-17

# local imports
import logging
import sqlite3
from db import Pipeline, Store
from runner import Runner
import util
import exception
from autodiscovery import PipelineScanner
import config

# python imports
import socket
from pathlib import Path
import struct
import json
from json.decoder import JSONDecodeError
import os
from datetime import datetime
import sys
from typing import Union, Any, Dict

# 3rd party imports
import daemon
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.base import ConflictingIdError


# manages all state and serializes changes through RPC calls
class GluetubeDaemon:

    def __init__(self) -> None:

        pass

    def start(self, debug: bool = False, fg: bool = False) -> None:

        dir_path = Path(__file__).parent.resolve()

        if fg:
            with daemon.DaemonContext(
                    working_directory=dir_path,
                    detach_process=False,
                    stdout=sys.stdout,
                    stderr=sys.stdout,
                    stdin=sys.stdin
                    ):
                self._setup(debug)

        try:
            gt_cfg = util.conf()
        except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
            raise exception.DaemonError(f"Failed to start daemon. {e}") from e
        log_file = open(gt_cfg.gluetube_log_file, "wb")

        with daemon.DaemonContext(
                working_directory=dir_path,
                stdout=log_file,
                stderr=log_file,
                detach_process=True
                ):
            self._setup(debug)

    # must setup everything after the daemon context, otherwise the daemon closes all file descriptors on me
    def _setup(self, debug: bool) -> None:

        if debug:
            sys.tracebacklimit = 1
        else:
            sys.tracebacklimit = 0

        # read filesystem dependencies: gluetube.cfg, gluetube.db, store.db
        try:
            gt_cfg = util.conf()
        except (exception.ConfigFileParseError, exception.ConfigFileNotFoundError) as e:
            raise exception.DaemonError(f"Failed to start daemon. {e}") from e

        try:
            db_p = Pipeline(db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_app_name), read_only=False)
        except exception.dbError as e:
            raise exception.DaemonError(f"Failed to start daemon. {e}") from e

        try:
            db_s = Store(gt_cfg.sqlite_token, db_path=Path(gt_cfg.sqlite_dir, gt_cfg.sqlite_kv_name), read_only=False)
        except exception.dbError as e:
            raise exception.DaemonError(f"Failed to start daemon. {e}") from e

        # setup daemon dependencies: write pid file, create apscheduler, create unix socket
        self._write_pid(Path(gt_cfg.pid_file))
        scheduler = self._setup_scheduler(101)
        sock = self._setup_listener_unix_socket(Path(gt_cfg.socket_file))

        # now populate scheduler and start it
        self._schedule_pipelines(scheduler, db_p, gt_cfg)
        self._schedule_auto_discovery(scheduler, gt_cfg)
        if not scheduler.running:
            scheduler.start()
        if not debug:
            logging.getLogger('apscheduler').setLevel('WARNING')

        self._main(scheduler, db_p, db_s, sock, debug, gt_cfg)

    # ###################################### DAEMON LOOP #######################################

    def _main(self, scheduler: BackgroundScheduler, db_p: Pipeline, db_s: Store, sock: socket.socket, debug: bool, gt_cfg: config.Gluetube) -> None:

        # keyword arguments for all RPC method calls
        kwargs = {'scheduler': scheduler, 'db_p': db_p, 'db_s': db_s, 'gt_cfg': gt_cfg}

        # main daemon loop, protect at all costs
        while True:

            # wait for incoming RPC messages
            conn, _ = sock.accept()

            # process message, get 4 byte length header
            raw_msg_len = self._recvall(conn, 4)
            if not raw_msg_len:
                logging.error("RPC call 4 byte length header missing")
                continue

            # get the full message and decode into a string
            msg_len = struct.unpack('>I', raw_msg_len)[0]
            msg = self._recvall(conn, msg_len).decode()

            # extract RPC details from json message
            try:
                msg = json.loads(msg)
            except JSONDecodeError as e:
                if debug:
                    logging.exception(f"RPC call failed. {e}. not valid json.")
                    continue
                else:
                    logging.error(f"RPC call failed. {e}. not valid json.")
                    continue
            try:
                func = msg['func']
                args = msg['params']
            except (KeyError, TypeError) as e:
                if debug:
                    logging.exception(f"RPC call failed. {e}")
                    continue
                else:
                    logging.error(f"RPC call failed. {e}")
                    continue
            # call rpc method
            try:
                getattr(self, func)(*args, **kwargs)
            except Exception as e:  # catch all exceptions, we don't want the daemon to crash
                if debug:
                    logging.exception(f"RPC call failed. {e}")
                    continue
                else:
                    logging.error(f"RPC call failed. {e}")
                    continue

    # #################################### END DAEMON LOOP #########################################

    def _write_pid(self, pid_file: Path) -> None:

        pid_file.write_text(str(os.getpid()), encoding="utf-8")

    def _setup_listener_unix_socket(self, socket_file: Path) -> socket.socket:

        # unix socket for IPC. for interfaces (cli, gui) to interact with daemon
        server_address = socket_file.resolve().as_posix()
        if socket_file.exists():
            socket_file.unlink()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(server_address)
        sock.listen()
        return sock

    def _setup_scheduler(self, max_threads: int) -> BackgroundScheduler:

        try:
            scheduler = BackgroundScheduler(
                {
                    'apscheduler.executors.default':
                    {'class': 'apscheduler.executors.pool:ThreadPoolExecutor', 'max_workers': f'{max_threads}'}
                }
            )
        except ValueError as e:
            raise exception.DaemonError(f"Failed to start daemon. Invalid max processes. {e}") from e
        return scheduler

    # Helper function to recv number of bytes or return None if EOF is hit
    def _recvall(self, sock: socket.socket, num_bytes: int) -> bytearray:

        data = bytearray()
        while len(data) < num_bytes:
            packet = sock.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def _schedule_pipelines(self, scheduler: BackgroundScheduler, db: Pipeline, gt_cfg: config.Gluetube) -> None:

        pipelines = db.all_pipelines_scheduling()
        for pipeline in pipelines:

            # if pipeline job is scheduled
            if scheduler.get_job(str(pipeline[4])):
                continue

            try:
                runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3], pipeline[4], gt_cfg)
            except exception.RunnerError as e:
                logging.error(f"{e}. Not scheduling pipeline, {pipeline[1]}, runner creation failed.")
                continue

            if pipeline[5]:  # if cron
                try:
                    scheduler.add_job(runner.run, CronTrigger.from_crontab(pipeline[5]), id=str(pipeline[4]))
                except ValueError as e:  # crontab validation failed
                    logging.error(f"Pipeline, {pipeline[1]}, not scheduled!. crontab incorrect: {pipeline[5]}. {e}")
            elif pipeline[6]:  # if run_date
                try:
                    scheduler.add_job(runner.run, DateTrigger(pipeline[6]), id=str(pipeline[4]))
                except ValueError as e:  # run_date validation failed
                    logging.error(f"Pipeline, {pipeline[1]}, no scheduled!. run_date incorrect: {pipeline[6]}. {e}")
            else:  # if no schedule, set dummy schedule
                scheduler.add_job(runner.run, DateTrigger(datetime(2999, 1, 1)), id=str(pipeline[4]))

            if pipeline[7]:  # if paused
                scheduler.get_job(str(pipeline[4])).pause()

    def _schedule_auto_discovery(self, scheduler: BackgroundScheduler, gt_cfg: config.Gluetube) -> None:

        interval = IntervalTrigger(seconds=int(gt_cfg.pipeline_scan_interval))

        try:
            pipeline_scanner = PipelineScanner(Path(gt_cfg.pipeline_dir), Path(gt_cfg.socket_file), db_dir=Path(gt_cfg.sqlite_dir), db_name=gt_cfg.sqlite_app_name)
        except exception.AutodiscoveryError as e:
            raise exception.DaemonError(f"Failed to initialize pipeline scanner. {e}") from e

        # if pipeline scanner job isn't scheduled at all
        if not scheduler.get_job('pipeline_scanner'):
            scheduler.add_job(pipeline_scanner.scan, interval, id='pipeline_scanner')

    # #################################################################################
    # RPC methods that are called from daemon loop when msg received from unix socket #
    # #################################################################################

    # ##### scheduler and database modifications together

    # auto-discovery calls this whenever a new pipeline.py AND pipeline_directory unique tuple is found
    def set_pipeline(self, name: str, py_name: str, dir_name: str, py_timestamp: str, **kwargs: Dict[str, Any]) -> None:

        try:
            pipeline_id = kwargs['db_p'].insert_pipeline(name, py_name, dir_name, py_timestamp)
            logging.info(f"Pipeline, {name}, added to database.")
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

        try:
            pipeline_schedule_id = kwargs['db_p'].insert_pipeline_schedule(pipeline_id)
            logging.info(f"Pipeline schedule for, {name}, added to database.")
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

        try:
            # job runs if no trigger is specified. So, I set a dummy date trigger for now to avoid job run
            self._schedule_add_job(pipeline_schedule_id, DateTrigger(datetime(2999, 1, 1)), kwargs['scheduler'], kwargs['db_p'], kwargs['gt_cfg'])
        except (ConflictingIdError, exception.RunnerError) as e:
            # rollback database insert
            kwargs['db_p'].delete_pipeline(pipeline_id)
            raise exception.DaemonError(f"Failed to add pipeline schedule. {e}") from e

    # auto-discovery calls this whenever a pipeline.py AND pipeline_directory unique tuple disappears
    def delete_pipeline(self, pipeline_id: int, **kwargs: Dict[str, Any]) -> None:

        schedules_id = kwargs['db_p'].pipeline_schedules_id(pipeline_id)

        # remove all schedules for the pipeline
        for an_id in schedules_id:
            try:
                kwargs['scheduler'].remove_job(str(an_id))
            except JobLookupError as e:
                raise exception.DaemonError(f"Failed to delete pipeline schedule. {e}") from e

        try:
            kwargs['db_p'].delete_pipeline(pipeline_id)
            logging.info(f"Deleted pipeline id {pipeline_id} from the database.")
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to delete pipeline from database. {e}") from e

    def set_schedule(self, pipeline_id: int, **kwargs: Dict[str, Any]) -> None:

        try:
            schedule_id = kwargs['db_p'].insert_pipeline_schedule(pipeline_id)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

        try:
            self._schedule_add_job(schedule_id, DateTrigger(datetime(2999, 1, 1)), kwargs['scheduler'], kwargs['db_p'], kwargs['gt_cfg'])
        except exception.RunnerError as e:
            raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

    def set_schedule_cron(self, schedule_id: int, cron: str, **kwargs: Dict[str, Any]) -> None:

        if kwargs['scheduler'].get_job(str(schedule_id)):
            try:
                kwargs['scheduler'].reschedule_job(str(schedule_id), trigger=CronTrigger.from_crontab(cron))
            except JobLookupError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e
        else:
            try:
                self._schedule_add_job(schedule_id, CronTrigger.from_crontab(cron), kwargs['scheduler'], kwargs['db_p'])
            except exception.RunnerError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

        # remove at if exists, then set cron in db
        try:
            if kwargs['db_p'].pipeline_schedule_at(schedule_id):
                kwargs['db_p'].update_pipeline_schedule_at(schedule_id, '')
            kwargs['db_p'].update_pipeline_schedule_cron(schedule_id, cron)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def set_schedule_at(self, schedule_id: int, at: str, **kwargs: Dict[str, Any]) -> None:

        # need to check if the job exists or not. Once a run-once job has been run, it's autoremoved from scheduler
        if kwargs['scheduler'].get_job(str(schedule_id)):
            try:
                kwargs['scheduler'].reschedule_job(str(schedule_id), trigger=DateTrigger(at))
            except JobLookupError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e
        else:
            try:
                self._schedule_add_job(schedule_id, DateTrigger(at), kwargs['scheduler'], kwargs['db_p'])
            except exception.RunnerError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

        # remove cron if exists, then set at in db
        try:
            if kwargs['db_p'].pipeline_schedule_cron(schedule_id):
                kwargs['db_p'].update_pipeline_schedule_cron(schedule_id, '')
            kwargs['db_p'].update_pipeline_schedule_at(schedule_id, at)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def set_schedule_now(self, schedule_id: int, **kwargs: Dict[str, Any]) -> None:

        if kwargs['scheduler'].get_job(str(schedule_id)):
            try:
                kwargs['scheduler'].reschedule_job(str(schedule_id))
            except JobLookupError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e
        else:
            try:
                self._schedule_add_job(schedule_id, None, kwargs['scheduler'], kwargs['db_p'])
            except exception.RunnerError as e:
                raise exception.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

    # remove cron and at if exists
        try:
            if kwargs['db_p'].pipeline_schedule_cron(schedule_id):
                kwargs['db_p'].update_pipeline_schedule_cron(schedule_id, '')
            if kwargs['db_p'].pipeline_schedule_at(schedule_id):
                kwargs['db_p'].update_pipeline_schedule_at(schedule_id, '')
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def delete_schedule(self, schedule_id: int, **kwargs: Dict[str, Any]) -> None:

        if kwargs['scheduler'].get_job(str(schedule_id)):
            kwargs['scheduler'].remove_job(str(schedule_id))

        try:
            kwargs['db_p'].delete_pipeline_schedule(schedule_id)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    # ##### database writes

    def set_schedule_latest_run(self, schedule_id: int, pipeline_run_id: int, **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_p'].update_pipeline_schedule_latest_run(schedule_id, pipeline_run_id)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def set_pipeline_run(self, pipeline_id: int, schedule_id: int, status: str, start_time: str, **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_p'].insert_pipeline_run(pipeline_id, schedule_id, status, start_time)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the status it's in
    def set_pipeline_run_status(self, pipeline_run_id: int, status: str, **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_p'].update_pipeline_run_status(pipeline_run_id, status)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the stage it's in
    def set_pipeline_run_stage_and_stage_msg(self, pipeline_run_id: int, stage: int, msg: str, **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_p'].update_pipeline_run_stage_and_stage_msg(pipeline_run_id, stage, msg)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    # runner.py calls this to update the pipeline run when it's done
    def set_pipeline_run_finished(self, pipeline_run_id: int, status: str, msg: str, end_time: str, **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_p'].update_pipeline_run_status_exit_msg_end_time(pipeline_run_id, status, msg, end_time)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def set_key_value(self, key: str, value: str, table: str = 'common', **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_s'].insert_key_value(table, key, value)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    def delete_key(self, key: str, table: str = 'common', **kwargs: Dict[str, Any]) -> None:

        try:
            kwargs['db_s'].delete_key(table, key)
        except sqlite3.Error as e:
            raise exception.DaemonError(f"Failed to update database. {e}") from e

    # ##### rpc helper methods

    def _schedule_add_job(self, schedule_id: int, trigger: Union[CronTrigger, DateTrigger, None],
                          scheduler: BackgroundScheduler = None, db_p: Pipeline = None, gt_cfg: config.Gluetube = None) -> None:

        pipeline = db_p.pipeline_from_schedule_id(schedule_id)

        try:
            runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3], schedule_id, gt_cfg)
        except exception.RunnerError(f"Not scheduling pipeline {pipeline[1]}, runner creation failed."):
            raise

        try:
            scheduler.add_job(runner.run, trigger=trigger, id=str(schedule_id))
        except ConflictingIdError as e:
            raise
