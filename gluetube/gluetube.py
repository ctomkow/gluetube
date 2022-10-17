#!/usr/bin/env python3

# Craig Tomkow
# 2022-08-03

# local imports
import command
import config
import util

# python imports
import logging
import argparse
import os

# 3rd party imports


class Gluetube:

    def __init__(self) -> None:

        self._setup_logging()
        args = self.parse_args(self._read_local_file('VERSION'))

        gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        if args.init:
            command.init_gluetube()
        elif args.ls:
            for name in command.ls_pipelines():
                print(name[0])
        elif args.run:
            try:
                command.run_pipeline(args.run, gt_cfg.pipeline_dir)
            except Exception as e:
                logging.exception(e)
                raise SystemExit(1)
        elif args.start:
            command.start_daemon()

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
        start_parser = parser.add_subparsers(dest='start')
        start_parser.add_parser('start', description='start gluetube as a daemon process')
        return parser.parse_args()

# helper functions

    def _read_local_file(self, file_name: str) -> str:

        with open(os.path.join(os.path.dirname(
                os.path.realpath(__file__)), file_name)) as file:
            return file.read().strip()

    def _setup_logging(self) -> None:

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s",
                            datefmt="%Y.%m.%d %H:%M:%S")


if __name__ == '__main__':
    Gluetube()
