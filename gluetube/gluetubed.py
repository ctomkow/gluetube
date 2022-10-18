# Craig Tomkow
# 2022-10-17

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

    while True:
        sleep(0.1)
        # TODO: implement python schedule library for running pipelines. https://pypi.org/project/schedule/
        # alternatives: 
        #       https://github.com/Miksus/rocketry/
        #       https://github.com/coleifer/huey
