# Craig Tomkow
# 2022-10-17

# local imports
import logging
from db import Pipeline
from runner import Runner
import config
import util

# python imports
import socket
from pathlib import Path
import struct
import json

# 3rd party imports
import daemon
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError


class GluetubeDaemon:

    _db = None
    _scheduler = None

    def __init__(self) -> None:

        pass

    def start(self, fg: bool = False) -> None:

        if fg:  # a hack, def needs docker --init for SIGnals, also have SIG handling for when docker propogates the SIGnals down
            self._main()

        # TODO: specify a PID file. So we know how to reference the process to shut it down later
        with daemon.DaemonContext():
            self._main()

    def _main(self) -> None:

        # setup
        self._db = Pipeline('gluetube.db', read_only=False)
        self._scheduler = BackgroundScheduler()
        gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))

        # unix socket for IPC. for interfaces (cli, gui) to interact with daemon
        server_address = gt_cfg.socket_file
        if Path(gt_cfg.socket_file).exists():
            Path(gt_cfg.socket_file).unlink()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(server_address)
        sock.listen()

        # initially schedule all pipelines from the database on daemon startup
        all_pipelines = self._db.pipeline_run_details()
        for pipeline in all_pipelines:

            # if pipeline job isn't scheduled at all
            if not self._scheduler.get_job(pipeline[0]):
                try:
                    self._scheduler.add_job(
                        Runner(pipeline[0], pipeline[1], pipeline[2]).run,
                        CronTrigger.from_crontab(pipeline[3]),
                        id=pipeline[0]
                        )
                except ValueError as e:  # crontab validation failed
                    logging.error(f"Failed to schedule pipline, {pipeline[0]}, crontab incorrect: {e}")

        if not self._scheduler.running:
            self._scheduler.start()

        # main daemon loop
        while True:

            conn, _ = sock.accept()

            # process request
            raw_msg_len = self._recvall(conn, 4)
            if not raw_msg_len:
                return None  # TODO: handle malformed input
            msg_len = struct.unpack('>I', raw_msg_len)[0]
            msg = self._recvall(conn, msg_len).decode()

            # TODO: try/except to validate JSON & keys exist to protect the daemon!
            msg = json.loads(msg)
            func = msg['function']
            args = msg['parameters']
            # call rpc method
            getattr(self, func)(*args)

    def _recvall(self, sock, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    # RPC methods that are called from daemon loop when msg received from unix socket
    #
    # these methods should update scheduler AND database (if applicable)

    def set_cron(self, pipeline_name: str, crontab: str) -> None:
        try:
            self._scheduler.modify_job(pipeline_name, trigger=CronTrigger.from_crontab(crontab))
        except JobLookupError:
            raise  # TODO: return a string of json error that will be returned to the unix socket
        self._db.pipeline_set_cron(pipeline_name, crontab)
