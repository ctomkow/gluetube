# Craig Tomkow
# 2022-10-17

# local imports
import logging
from db import Pipeline
from runner import Runner

# python imports
from time import sleep

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

        db = Pipeline('gluetube.db')
        self.scheduler = BackgroundScheduler()

        while True:

            all_pipelines = db.pipeline_run_details()

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

            sleep(30)

    # def _update_schedule(self, id: str, crontab: str) -> None:

    #     self.scheduler.modify_job(id, trigger=CronTrigger.from_crontab(crontab))
