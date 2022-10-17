#!/usr/bin/env python3

# Craig Tomkow
# 2022-08-03

# local imports
import command

# python imports
import logging
import argparse
import os

# 3rd party imports


class Gluetube:

    def __init__(self) -> None:

        self._setup_logging()
        args = self.parse_args(self._read_local_file('VERSION'))

        if args.init:
            command.init_gluetube()
        elif args.ls:
            for name in command.ls_pipelines():
                print(name[0])
        elif args.run:
            try:
                command.run_pipeline(args.run, self._conf_dir(), self._pipeline_dir())
            except Exception as e:
                logging.exception(e)
                raise SystemExit(1)

        # gracefully exit
        raise SystemExit(0)

    def parse_args(self, version: str) -> argparse.Namespace:

        parser = argparse.ArgumentParser(
            description="Runs pipelines between systems"
        )
        # all flags here
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-v', '--version', action='version', version=f"%(prog)s {version}")
        group.add_argument('-l', '--ls', action='store_true', help='List all available pipelines')
        group.add_argument('-r', '--run', action='store', metavar='NAME',
                           help='Provide the pipeline name to run. e.g. pipeline --run my_pipeline')
        group.add_argument('-i', '--init', action='store_true', help='Setup gluetube for the first time (db setup, etc)')
        return parser.parse_args()

# helper functions

    def _read_local_file(self, file_name: str) -> str:

        with open(os.path.join(os.path.dirname(
                os.path.realpath(__file__)), file_name)) as file:
            return file.read().strip()

    def _setup_logging(self) -> None:

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s",
                            datefmt="%Y.%m.%d %H:%M:%S")

    # all the possible directories for the cfg files,
    #   depending on how things are packaged and deployed
    #   starts locally, then branches out eventually system-wide
    def _conf_dir(self) -> list:

        return [
            './',
            'cfg/',
            '~/.gluetube/cfg/',
            '/usr/local/etc/gluetube/',
            '/etc/opt/gluetube/',
            '/etc/gluetube/'
        ]

    def _pipeline_dir(self) -> list:
        # TODO: fix this, as it needs an absolute path for the subprocess trigger to work
        return [
            '/home/gluetube/.gluetube/pipelines/'
        ]


if __name__ == '__main__':
    Gluetube()
