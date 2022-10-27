# Craig Tomkow
# 2022-10-17

# local imports
import logging
import sqlite3
from db import Pipeline
from runner import Runner
import config
import util
import exceptions
from autodiscovery import PipelineScanner

# python imports
import socket
from pathlib import Path
import struct
import json
from json.decoder import JSONDecodeError
import os
from datetime import datetime

# 3rd party imports
import daemon
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.base import ConflictingIdError


class GluetubeDaemon:

    _db = None
    _scheduler = None
    _sock = None
    _debug = None

    def __init__(self) -> None:

        pass

    def start(self, debug: bool = False, fg: bool = False) -> None:

        if fg:  # TODO: a hack, needs docker --init for SIGnals, also SIG handling when docker propogates the SIGnals down
            self._setup(debug)

        # TODO: send logs to proper location of daemon
        dir_path = Path(__file__).parent.resolve()
        with daemon.DaemonContext(
                working_directory=dir_path,
                stdout=open("./stdout.log", "wb"), stderr=open("./stderr.log", "wb")
                ):
            self._setup(debug)

    # must setup everything after the daemon context, otherwise the daemon closes all file descriptors on me
    def _setup(self, debug: bool) -> None:

        self._write_pid()

        self._debug = debug
        # setup
        try:
            self._db = Pipeline('gluetube.db', read_only=False)
        except exceptions.dbError as e:
            raise exceptions.DaemonError(f"Failed to start daemon. {e}") from e

        self._scheduler = BackgroundScheduler()

        try:
            gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
            raise exceptions.DaemonError(f"Failed to start daemon. {e}") from e

        # unix socket for IPC. for interfaces (cli, gui) to interact with daemon
        server_address = gt_cfg.socket_file
        if Path(gt_cfg.socket_file).exists():
            Path(gt_cfg.socket_file).unlink()
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(server_address)
        self._sock.listen()

        # initially schedule all pipelines from the database on daemon startup
        self._schedule_pipelines(self._scheduler, self._db)

        # schedule the pipeline scanner for pipeline auto-discovery
        self._schedule_auto_discovery(self._scheduler)

        if not self._scheduler.running:
            self._scheduler.start()

        logging.getLogger('apscheduler').setLevel('WARNING')

        self._main(self._scheduler, self._db, self._sock, self._debug)

    ####################################### DAEMON LOOP #######################################

    def _main(self, scheduler: BackgroundScheduler, db: Pipeline, sock: socket.socket, debug: bool) -> None:

        # keyword arguments for all RPC method calls
        kwargs = {'scheduler': scheduler, 'db': db}

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
                func = msg['function']
                args = msg['parameters']
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

    ##################################### END DAEMON LOOP #########################################

    def _write_pid(self) -> None:

        with open('/tmp/gluetube.pid', 'w', encoding="utf-8") as f:
            f.write(str(os.getpid()))

    # Helper function to recv number of bytes or return None if EOF is hit
    def _recvall(self, sock: socket.socket, num_bytes: int) -> bytearray:

        data = bytearray()
        while len(data) < num_bytes:
            packet = sock.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def _schedule_pipelines(self, scheduler: BackgroundScheduler, db: Pipeline) -> None:

        pipelines = db.pipeline_run_details()
        for pipeline in pipelines:

            cron = None
            try:
                cron = CronTrigger.from_crontab(pipeline[4])
            except ValueError as e:  # crontab validation failed
                logging.error(f"Pipeline, {pipeline[1]}, will not run!. crontab incorrect: {pipeline[4]}. {e}")

            try:
                runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3])
            except exceptions.RunnerError as e:
                logging.error(f"{e}. Not scheduling pipeline, {pipeline[1]}, runner creation failed.")
                continue

            # if pipeline job isn't scheduled at all
            if not scheduler.get_job(str(pipeline[0])):
                if cron:
                    scheduler.add_job(runner.run, cron, id=str(pipeline[0]))
                else:
                    scheduler.add_job(runner.run, trigger=DateTrigger(datetime(2999, 1, 1)), id=str(pipeline[0]))

    def _schedule_auto_discovery(self, scheduler: BackgroundScheduler) -> None:

        try:
            gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
            raise exceptions.DaemonError(f"Failed to schedule auto-discovery. {e}") from e

        interval = IntervalTrigger(seconds=int(gt_cfg.pipeline_scan_interval))

        pipeline_scanner = PipelineScanner(gt_cfg.pipeline_dir)

        # if pipeline scanner job isn't scheduled at all
        if not scheduler.get_job('pipeline_scanner'):
            scheduler.add_job(pipeline_scanner.scan, interval, id='pipeline_scanner')

    # ###############################################################################################
    # RPC methods that are called from daemon loop when msg received from unix socket
    #
    # RPC methods should be writing to the database and interacting with the schedule (if applicable)
    # these methods should update scheduler AND database at the same time (if applicable)
    # therefore, all RPC methods should include the scheduler and db keyword arguments

    def set_cron(self, pipeline_id: int, crontab: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:
        try:
            scheduler.reschedule_job(str(pipeline_id), trigger=CronTrigger.from_crontab(crontab))
        except JobLookupError as e:
            raise exceptions.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

        try:
            db.pipeline_set_cron(pipeline_id, crontab)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # auto-discovery calls this whenever a new pipeline.py AND pipeline_directory unique tuple is found
    def set_pipeline(self, name: str, py_name: str, dir_name: str, crontab: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.pipeline_insert(name, py_name, dir_name, crontab)
            logging.info(f"Pipeline, {name}, added to database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

        try:
            pipeline_id = db.pipeline_id_from_tuple(py_name, dir_name)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to get id from database. {e}") from e

        try:
            runner = Runner(pipeline_id, name, py_name, dir_name)
        except exceptions.RunnerError as e:
            raise exceptions.DaemonError(f"{e}. Not scheduling pipeline, {name}, runner creation failed.") from e

        try:
            if crontab:
                scheduler.add_job(runner.run, trigger=CronTrigger.from_crontab(crontab), id=str(pipeline_id))
            else:  # job runs if no trigger is specified. So, I set a dummy date trigger for now to avoid job run
                scheduler.add_job(runner.run, trigger=DateTrigger(datetime(2999, 1, 1)), id=str(pipeline_id))
        except ConflictingIdError as e:
            # rollback database insert
            db.pipeline_delete(pipeline_id)
            raise exceptions.DaemonError(f"Failed to add pipeline schedule. {e}") from e

    # auto-discovery calls this whenever a pipeline.py AND pipeline_directory unique tuple disappears
    def delete_pipeline(self, pipeline_id: int, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            scheduler.remove_job(str(pipeline_id))
        except JobLookupError as e:
            raise exceptions.DaemonError(f"Failed to delete pipeline schedule. {e}") from e

        try:
            db.pipeline_delete(pipeline_id)
            logging.info(f"Deleted pipeline id {pipeline_id} from the database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to delete pipeline from database. {e}") from e

    # pipeline.py calls this to update the status it's in
    def set_status(self, pipeline_id: int, status: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.pipeline_set_status(pipeline_id, status)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the stage it's in
    def set_stage(self, pipeline_id: int, stage: int, msg: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.pipeline_set_stage(pipeline_id, stage, msg)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the stacktrace when pipeline crashes
    def set_stacktrace(self, pipeline_id: int, stacktrace: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.pipeline_set_stacktrace(pipeline_id, stacktrace)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e