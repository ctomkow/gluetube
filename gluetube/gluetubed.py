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


class GTdaemon:

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
        self.db = Pipeline('gluetube.db')
        self.scheduler = BackgroundScheduler()
        gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))

        # unix socket for IPC. for interfaces (cli, gui) to interact with daemon
        server_address = gt_cfg.socket_file
        if Path(gt_cfg.socket_file).exists():
            Path(gt_cfg.socket_file).unlink()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(server_address)
        sock.listen()

        # initially schedule all pipelines from the database on daemon startup
        all_pipelines = self.db.pipeline_run_details()
        for pipeline in all_pipelines:

            # if pipeline job isn't scheduled at all
            if not self.scheduler.get_job(pipeline[0]):
                try:
                    self.scheduler.add_job(
                        Runner(pipeline[0], pipeline[1], pipeline[2]).run,
                        CronTrigger.from_crontab(pipeline[3]),
                        id=pipeline[0]
                        )
                except ValueError as e:  # crontab validation failed
                    logging.error(f"Failed to schedule pipline, {pipeline[0]}, crontab incorrect: {e}")

        if not self.scheduler.running:
            self.scheduler.start()

        while True:

            conn, client_addr = sock.accept()

            # process request
            raw_msg_len = self._recvall(conn, 4)
            if not raw_msg_len:
                return None
            msg_len = struct.unpack('>I', raw_msg_len)[0]
            msg = self._recvall(conn, msg_len).decode()
            print(msg)
            # TODO: finish parsing incoming msg. It will be a json string, json.loads(msg) and extract 'function'

    def _recvall(self, sock, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    # rpc methods that are to be called from the daemon when msg received of function name from unix socket

    def _update_schedule_cron(self, id: str, crontab: str) -> None:

        self.scheduler.modify_job(id, trigger=CronTrigger.from_crontab(crontab))
        self.db.pipeline_update_cron(id, crontab)
