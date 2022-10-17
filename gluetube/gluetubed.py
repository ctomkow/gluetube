# Craig Tomkow
# 2022-10-17

# python imports
from time import sleep

# 3rd party imports
import daemon


def start() -> None:

    with daemon.DaemonContext():
        _main()


def _main() -> None:

    while True:
        sleep(0.1)

        # TODO: implement python schedule library for running pipelines. https://pypi.org/project/schedule/
