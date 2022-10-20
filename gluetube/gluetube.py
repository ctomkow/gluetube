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

        # gluetube level
        if args.init:
            command.init_gluetube()
        elif args.ls:
            for name in command.ls_pipelines():
                print(name[0])
        elif args.dev:
            try:
                command.dev_msg_to_daemon(args.dev)
            except FileNotFoundError as e:
                logging.exception(e)
                raise SystemExit(1)
            except ConnectionRefusedError as e:
                logging.error(f"{e}. Is the daemon running?")
                raise SystemExit(1)
        elif 'DAEMON' in args:  # gluetube daemon level
            if args.foreground:
                command.start_daemon_fg()
            elif args.background:
                command.start_daemon_bg()
        elif 'PIPELINE' in args:  # gluetube pipeline level
            if args.run:
                try:
                    command.run_pipeline(args.PIPELINE[0])
                except Exception as e:
                    logging.exception(e)
                    raise SystemExit(1)
            elif args.cron:
                try:
                    command.pipeline_set_cron(args.PIPELINE[0], args.cron)
                except ConnectionRefusedError as e:
                    logging.error(f"{e}. Is the daemon running?")
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
        group.add_argument('-i', '--init', action='store_true', help='Setup gluetube for the first time (db setup, etc)')
        group.add_argument('-l', '--ls', action='store_true', help='List all available pipelines')
        group.add_argument('--dev', action='store', metavar='TESTMSG', help='Send test msg to daemon')

        sub_parser = parser.add_subparsers()
        daemon = sub_parser.add_parser('daemon', description='start gluetube as a daemon process')
        daemon_group = daemon.add_mutually_exclusive_group()
        daemon_group.add_argument('DAEMON', action='store', metavar='', nargs='?')
        daemon_group.add_argument('-f', '--foreground', action='store_true', help='run daemon in the foreground')
        daemon_group.add_argument('-b', '--background', action='store_true', help='run daemon in the background')

        pipeline = sub_parser.add_parser('pipeline', description='perform actions and updates to pipelines')
        pipeline.add_argument('PIPELINE', action='store', type=str, nargs=1, help='name of pipeline to act on')
        pipeline.add_argument('-r', '--run', action='store_true', help='manually run the pipeline once')
        pipeline.add_argument('--cron', action='store', metavar='CRON', help="set cron schedule e.g. '* * * * *'")
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
