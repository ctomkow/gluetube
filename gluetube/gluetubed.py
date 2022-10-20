# Craig Tomkow
# 2022-10-17

# local imports
import logging
from db import Pipeline
from runner import Runner
import config
import util
import exceptions

# python imports
import socket
from pathlib import Path
import struct
import json
from json.decoder import JSONDecodeError

# 3rd party imports
import daemon
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError


class GluetubeDaemon:

    _db = None
    _scheduler = None
    _sock = None

    def __init__(self) -> None:

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

        if not self._scheduler.running:
            self._scheduler.start()

    def start(self, fg: bool = False) -> None:

        if fg:  # a hack, def needs docker --init for SIGnals, also have SIG handling for when docker propogates the SIGnals down
            self._main(self._scheduler, self._db, self._sock)

        # TODO: specify a PID file. So we know how to reference the process to shut it down later
        with daemon.DaemonContext():
            self._main(self._scheduler, self._db, self._sock)

    def _main(self, scheduler: BackgroundScheduler, db: Pipeline, sock: socket.socket) -> None:

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
                logging.error(f"RPC call failed. {e}. not valid json.")
                continue
            try:
                func = msg['function']
                args = msg['parameters']
            except KeyError as e:
                logging.error(f"RPC call failed. '{e}' key not found.")
                continue
            # call rpc method
            try:
                getattr(self, func)(*args, **kwargs)
            except Exception as e:
                logging.error(f"RPC call failed. {e}.")
                continue

    # Helper function to recv number of bytes or return None if EOF is hit
    def _recvall(self, sock: socket.socket, num_bytes: int) -> bytearray:

        data = bytearray()
        while len(data) < num_bytes:
            packet = sock.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def _schedule_pipelines(self, scheduler: BackgroundScheduler, db: Pipeline):

        pipelines = db.pipeline_run_details()
        for pipeline in pipelines:

            try:
                cron = CronTrigger.from_crontab(pipeline[3])
            except ValueError as e:  # crontab validation failed
                logging.error(f"Not scheduling pipline, {pipeline[0]}, crontab incorrect: {e}")
                continue

            try:
                runner = Runner(pipeline[0], pipeline[1], pipeline[2])
            except exceptions.RunnerError as e:
                logging.error(f"{e}. Not scheduling pipeline, {pipeline[0]}, runner creation failed.")
                continue

            # if pipeline job isn't scheduled at all
            if not scheduler.get_job(pipeline[0]):
                scheduler.add_job(runner.run, cron, id=pipeline[0])

    # ###############################################################################
    # RPC methods that are called from daemon loop when msg received from unix socket
    #
    # these methods should update scheduler AND database
    # therefore, all RPC methods should include the scheduler and db keyword arguments

    def set_cron(self, pipeline_name: str, crontab: str, scheduler: BackgroundScheduler = None, db: Pipeline = None) -> None:
        try:
            scheduler.modify_job(pipeline_name, trigger=CronTrigger.from_crontab(crontab))
        except JobLookupError:
            raise
        db.pipeline_set_cron(pipeline_name, crontab)
        # TODO: try/except the db call, if it fails, then call db for existing schedule, and set the old schedule (e.g. rollback the change)
