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


# manages all state and serializes changes through RPC calls
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

        self._scheduler = BackgroundScheduler(
            {
                'apscheduler.executors.default':
                {'class': 'apscheduler.executors.pool:ThreadPoolExecutor', 'max_workers': '101'}
            }
        )

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

    # ###################################### DAEMON LOOP #######################################

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

    # #################################### END DAEMON LOOP #########################################

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

        pipelines = db.all_pipelines_scheduling()
        for pipeline in pipelines:

            # if pipeline job is scheduled
            if scheduler.get_job(str(pipeline[4])):
                continue

            try:
                runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3])
            except exceptions.RunnerError as e:
                logging.error(f"{e}. Not scheduling pipeline, {pipeline[1]}, runner creation failed.")
                continue

            if pipeline[5]:  # if cron
                try:
                    scheduler.add_job(runner.run, CronTrigger.from_crontab(pipeline[5]), id=str(pipeline[4]))
                except ValueError as e:  # crontab validation failed
                    logging.error(f"Pipeline, {pipeline[1]}, not scheduled!. crontab incorrect: {pipeline[5]}. {e}")
            elif pipeline[6]:  # if run_date
                try:  # TODO: validate that ISO 8601 timestamp works for datetrigger
                    scheduler.add_job(runner.run, DateTrigger(pipeline[6]), id=str(pipeline[4]))
                except ValueError as e:  # run_date validation failed
                    logging.error(f"Pipeline, {pipeline[1]}, no scheduled!. run_date incorrect: {pipeline[6]}. {e}")
            else:  # if no schedule, set dummy schedule
                scheduler.add_job(runner.run, DateTrigger(datetime(2999, 1, 1)), id=str(pipeline[4]))

            if pipeline[7]:  # if paused
                scheduler.get_job(str(pipeline[4])).pause()

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
    # RPC 'set' and 'delete' methods are writing to the database and interacting with the scheduler
    # RPC '_insert' and '_update' methods are solely db writes

    # ##### 'set' and 'delete' methods

    # auto-discovery calls this whenever a new pipeline.py AND pipeline_directory unique tuple is found
    def set_new_pipeline(self, name: str, py_name: str, dir_name: str, py_timestamp: str,
                         scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            pipeline_id = db.insert_pipeline(name, py_name, dir_name, py_timestamp)
            logging.info(f"Pipeline, {name}, added to database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

        try:
            pipeline_schedule_id = db.insert_pipeline_schedule(pipeline_id)
            logging.info(f"Pipeline schedule for, {name}, added to database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

        # TODO: db call to gluetube app table, get if run_immediately is set or not

        try:
            runner = Runner(pipeline_id, name, py_name, dir_name)
        except exceptions.RunnerError as e:
            raise exceptions.DaemonError(f"{e}. Not scheduling pipeline, {name}, runner creation failed.") from e

        try:
            # if run_immediately:
            #     scheduler.add_job(runner.run, id=str(pipeline_schedule_id))

            # job runs if no trigger is specified. So, I set a dummy date trigger for now to avoid job run
            scheduler.add_job(runner.run, trigger=DateTrigger(datetime(2999, 1, 1)), id=str(pipeline_schedule_id))
        except ConflictingIdError as e:
            # rollback database insert
            db.delete_pipeline(pipeline_id)
            raise exceptions.DaemonError(f"Failed to add pipeline schedule. {e}") from e

    def set_pipeline_latest_run(self, pipeline_id: int, pipeline_run_id: int,
                                scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_latest_run(pipeline_id, pipeline_run_id)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # auto-discovery calls this whenever a pipeline.py AND pipeline_directory unique tuple disappears
    def delete_pipeline(self, pipeline_id: int, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        schedules_id = db.pipeline_schedules_id(pipeline_id)

        # remove all schedules for the pipeline
        for an_id in schedules_id:
            try:
                scheduler.remove_job(str(an_id))
            except JobLookupError as e:
                raise exceptions.DaemonError(f"Failed to delete pipeline schedule. {e}") from e

        try:
            db.delete_pipeline(pipeline_id)
            logging.info(f"Deleted pipeline id {pipeline_id} from the database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to delete pipeline from database. {e}") from e

    def set_schedule_cron(self, schedule_id: int, cron: str,
                          scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:
        # TODO: handle when reschedule works but db call fails and vice versa
        try:
            scheduler.reschedule_job(str(schedule_id), trigger=CronTrigger.from_crontab(cron))
        except JobLookupError as e:
            raise exceptions.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

        # remove run_date if exists, then set cron in db
        try:
            if db.pipeline_schedule_run_date(schedule_id):
                db.update_pipeline_schedule_run_date(schedule_id, '')
            db.update_pipeline_schedule_cron(schedule_id, cron)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def set_schedule_at(self, schedule_id: int, run_date_time: str,
                        scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:
        # TODO: handle when reschedule works but db call fails and vice versa
        try:
            scheduler.reschedule_job(str(schedule_id), trigger=DateTrigger(run_date_time))
        except JobLookupError as e:
            raise exceptions.DaemonError(f"Failed to modify pipeline schedule. {e}") from e

        # remove cron if exists, then set run_date in db
        try:
            if db.pipeline_schedule_cron(schedule_id):
                db.update_pipeline_schedule_cron(schedule_id, '')
            db.update_pipeline_schedule_run_date(schedule_id, run_date_time)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # Steps
    #   1. get all pipeline details from db
    #   2. create runner object
    #   3. insert new schedule into db
    #   4. schedule a dummy job into scheduler
    # TODO: handle when reschedule works but db call fails and vice versa
    def set_schedule_new(self, pipeline_id: int, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        pipeline = db.pipeline(pipeline_id)

        try:
            runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3])
        except exceptions.RunnerError as e:
            logging.error(f"{e}. Not scheduling pipeline, {pipeline[1]}, runner creation failed.")

        try:
            schedule_id = db.insert_pipeline_schedule(pipeline_id)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

        scheduler.add_job(runner.run, DateTrigger(datetime(2999, 1, 1)), id=str(schedule_id))

    def set_pipeline_run(self, pipeline_id: int, status: str, start_time: str,
                         scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.insert_pipeline_run(pipeline_id, status, start_time)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the status it's in
    def set_pipeline_run_status(self, pipeline_run_id: int, status: str,
                                scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_status(pipeline_run_id, status)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline.py calls this to update the stage it's in
    def set_pipeline_run_stage_and_stage_msg(self, pipeline_run_id: int, stage: int, msg: str,
                                             scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_stage_and_stage_msg(pipeline_run_id, stage, msg)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # runner.py calls this to update the pipeline run when it's done
    def set_pipeline_run_finished(self, pipeline_run_id: int, status: str, msg: str, end_time: str,
                                  scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_status_exit_msg_end_time(pipeline_run_id, status, msg, end_time)
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline individual db writes

    def _insert_pipeline(self, name: str, py_name: str, dir_name: str, py_timestamp: str,
                         scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.insert_pipeline(name, py_name, dir_name, py_timestamp)
            logging.info(f"Pipeline, {name}, added to database.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_name(self, id: int, name: str,
                              scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_name(id, name)
            logging.info(f"Pipeline id, {str(id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_py_timestamp(self, id: int, timestamp: str,
                                      scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_py_timestamp(id, timestamp)
            logging.info(f"Pipeline id, {str(id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_latest_run(self, id: int, run_id: int,
                                    scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_latest_run(id, run_id)
            logging.info(f"Pipeline id, {str(id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline_schedule individual db writes

    def _insert_pipeline_schedule(self, pipeline_id: int, cron: str = '', run_date: str = '', paused: int = 0,
                                  retry_on_crash: int = 0, retry_num: int = 0, max_retries: int = 0,
                                  scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.insert_pipeline_schedule(pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries)
            logging.info(f"Schedule for pipline id, {str(pipeline_id)}, inserted.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_cron(self, schedule_id: int, cron: str,
                                       scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_cron(schedule_id, cron)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_run_date(self, schedule_id: int, run_date: str,
                                           scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_run_date(schedule_id, run_date)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_paused(self, schedule_id: int, paused: int,
                                         scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_paused(schedule_id, paused)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_retry_on_crash(self, schedule_id: int, retry_on_crash: int,
                                                 scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_retry_on_crash(schedule_id, retry_on_crash)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_retry_num(self, schedule_id: int, retry_num: int,
                                            scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_retry_num(schedule_id, retry_num)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_schedule_max_retries(self, schedule_id: int, max_retries: int,
                                              scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_schedule_max_retries(schedule_id, max_retries)
            logging.info(f"Pipeline schedule id, {str(schedule_id)}, updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    # pipeline_run individual db writes

    def _insert_pipeline_run(self, pipeline_id: int, status: str, start_time: str,
                             scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.insert_pipeline_run(pipeline_id, status, start_time)
            logging.info(f"Pipeline run for pipeline id: {str(pipeline_id)}, inserted.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_run_status(self, pipeline_run_id: int, status: str,
                                    scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_status(pipeline_run_id, status)
            logging.info(f"Pipeline run: {str(pipeline_run_id)}, status updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_run_stage(self, pipeline_run_id: int, stage: int,
                                   scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_stage(pipeline_run_id, stage)
            logging.info(f"Pipeline run: {str(pipeline_run_id)}, stage updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_run_stage_msg(self, pipeline_run_id: int, msg: str,
                                       scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_stage_msg(pipeline_run_id, msg)
            logging.info(f"Pipeline run: {str(pipeline_run_id)}, stage msg updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_run_exit_msg(self, pipeline_run_id: int, msg: str,
                                      scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_exit_msg(pipeline_run_id, msg)
            logging.info(f"Pipeline run: {str(pipeline_run_id)}, exit msg updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e

    def _update_pipeline_run_end_time(self, pipeline_run_id: int, end_time: str,
                                      scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:

        try:
            db.update_pipeline_run_end_time(pipeline_run_id, end_time)
            logging.info(f"Pipeline run: {str(pipeline_run_id)}, end time updated.")
        except sqlite3.Error as e:
            raise exceptions.DaemonError(f"Failed to update database. {e}") from e
