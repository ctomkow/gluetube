# Craig Tomkow
# 2022-10-17

# local imports
from db import Pipeline
from runner import Runner

# python imports
from time import sleep

# 3rd party imports
import daemon


def start(fg: bool = False) -> None:

    if fg:  # a hack, def needs docker --init for SIGnals, also have SIG handling for when docker propogates the SIGnals down
        _main()

    # TODO: specify a PID file. So we know how to reference the process to shut it down later
    with daemon.DaemonContext():
        _main()


def _main() -> None:

    db = Pipeline('gluetube.db')

    while True:

        # TODO: check if pipeline has been updated

        # TODO: need to track runners somehow, so we can gracefully kill them and restart them
        #       track in memory not db, if service is stopped, the schedulers are killed also, so everything dies, i think

        # TODO: need to check if a runner is running or not, as not to duplicate runners

        state = {}

        all_pipelines = db.pipeline_run_details()

        for pipeline in all_pipelines:
            # TODO: fork each runner
            runner = Runner(pipeline[0], pipeline[1], pipeline[2], pipeline[3])
            runner.run()

        sleep(1)
