#!/usr/bin/env python3

# Craig Tomkow
# 2022-08-03

# local imports
import command
import exception
import util

# python imports
import logging
import argparse
import os
from pathlib import Path

# 3rd party imports


class Gluetube:

    def __init__(self) -> None:

        self._setup_logging()
        args = self.parse_args(self._read_local_file('VERSION'))
        gt_cfg = util.conf()
        os.chdir(Path(__file__).parent.resolve())

        # gluetube level
        if args.init:
            command.gluetube_init()
        elif args.dev:
            try:
                command.gluetube_dev(args.dev, Path(gt_cfg.socket_file))
            except exception.rpcError as e:
                if args.debug:
                    logging.exception(f"Is the daemon running? {e}")
                else:
                    logging.error(f"Is the daemon running? {e}")
                raise SystemExit(1)
        elif args.scan:
            # TODO: try/except
            command.gluetube_scan()
        elif 'sub_cmd_summary' in args:  # gluetube summary sub-command level
            try:
                print(command.summary())
            except exception.dbError as e:
                if args.debug:
                    logging.exception(f"List pipelines failed. {e}")
                else:
                    logging.error(f"List pipelines failed. {e}")
                raise SystemExit(1)
        elif 'sub_cmd_daemon' in args:  # gluetube daemon sub-command level
            try:
                if args.foreground:
                    command.daemon_fg(args.debug)
                elif args.background:
                    command.daemon_bg(args.debug)
            except exception.DaemonError as e:
                if args.debug:
                    logging.exception(f"Daemon failure. {e}")
                else:
                    logging.critical(f"Daemon failure. {e}")
                raise SystemExit(1)
        elif 'sub_cmd_pipeline' in args:  # gluetube pipeline sub-command level
            try:
                if args.run:
                    command.pipeline_run(args.NAME[0])
                elif args.schedule:
                    command.pipeline_schedule(args.NAME[0], Path(gt_cfg.socket_file))
            except (exception.dbError, exception.RunnerError) as e:
                if args.debug:
                    logging.exception(f"Pipeline run failure. {e}")
                else:
                    logging.critical(f"Pipeline run failure. {e}")
                raise SystemExit(1)
        elif 'sub_cmd_schedule' in args:  # gluetube schedule sub-command level
            try:
                if args.cron:
                    command.schedule_cron(args.id, args.cron, Path(gt_cfg.socket_file))
                elif args.at:
                    command.schedule_at(args.id, args.at, Path(gt_cfg.socket_file))
            except exception.rpcError as e:
                if args.debug:
                    logging.exception(f"Is the daemon running? {e}")
                else:
                    logging.error(f"Is the daemon running? {e}")
                raise SystemExit(1)
        elif 'sub_cmd_store' in args:  # gluetube store sub-command level
            try:
                if args.add:
                    command.store_add(args.KEY, args.VALUE, Path(gt_cfg.socket_file))
                elif args.delete:
                    command.store_del(args.KEY, Path(gt_cfg.socket_file))
                elif args.ls:
                    print(command.store_ls())
            except exception.rpcError as e:
                if args.debug:
                    logging.exception(f"Is the daemon running? {e}")
                else:
                    logging.error(f"Is the daemon running? {e}")
                raise SystemExit(1)
        # gracefully exit
        raise SystemExit(0)

    def parse_args(self, version: str) -> argparse.Namespace:

        parser = argparse.ArgumentParser(
            description="Runs pipelines between systems"
        )
        # all flags here
        parser.add_argument('--debug', action='store_true', help='Will print out more verbose logging, stacktrace, etc')

        group = parser.add_mutually_exclusive_group()
        group.add_argument('-v', '--version', action='version', version=f"%(prog)s {version}")
        group.add_argument('-i', '--init', action='store_true', help='Setup gluetube for the first time (db setup, etc)')
        group.add_argument('--dev', action='store', metavar='TESTMSG', help='Send test msg to daemon')
        group.add_argument('--scan', action='store_true', help='manually scan pipeline directory for pipelines')

        sub_parser = parser.add_subparsers()

        summary = sub_parser.add_parser('summary', description='show summary of pipelines, schedules, and runs')
        summary.add_argument('sub_cmd_summary', metavar='', default=True, nargs='?')  # a hidden tag to identify sub cmd

        daemon = sub_parser.add_parser('daemon', description='start gluetube as a daemon process')
        daemon.add_argument('sub_cmd_daemon', metavar='', default=True, nargs='?')  # a hidden tag to identify sub cmd
        daemon_group = daemon.add_mutually_exclusive_group()
        daemon_group.add_argument('-f', '--foreground', action='store_true', help='run daemon in the foreground')
        daemon_group.add_argument('-b', '--background', action='store_true', help='run daemon in the background')
        # TODO: gluetube daemon -s --stop. Needs a PID to be tracked in the daemon, also specify it's location in gluetube.cfg

        pipeline = sub_parser.add_parser('pipeline', description='perform actions and updates to pipelines')
        pipeline.add_argument('sub_cmd_pipeline', metavar='', default=True, nargs='?')  # a hidden tag to identify sub cmd
        pipeline.add_argument('NAME', action='store', type=str, nargs=1, help='name of pipeline to act on')
        pipeline.add_argument('-r', '--run', action='store_true', help='manually run the pipeline once')
        pipeline.add_argument('--schedule', action='store_true', help='create a new blank pipeline schedule')

        schedule = sub_parser.add_parser('schedule', description='perform actions and updates to existing schedules')
        schedule.add_argument('sub_cmd_schedule', metavar='', default=True, nargs='?')  # a hidden tag to identify sub cmd
        schedule.add_argument('--id', action='store', metavar='ID', type=int, help='id of schedule to modify')
        schedule_group = schedule.add_mutually_exclusive_group()
        schedule_group.add_argument('--cron', action='store', metavar='CRON', help="set cron schedule e.g. '* * * * *'")
        schedule_group.add_argument('--at', action='store', metavar='AT', help="run on a date/time (ISO 8601) e.g. '2022-10-01 00:00:00'")

        store = sub_parser.add_parser('store', description='add and remove key value pairs')
        store.add_argument('sub_cmd_store', metavar='', default=True, nargs='?')  # a hidden tag to identify sub cmd
        store.add_argument('KEY', action='store', type=str, nargs='?', help='name of key to act on')
        store.add_argument('VALUE', action='store', type=str, nargs='?', help='the value to be stored with the key')
        store_group = store.add_mutually_exclusive_group()
        store_group.add_argument('--add', action='store_true', help='add the key and value pair to the encrypted database')
        store_group.add_argument('--delete', action='store_true', help='delete the key from the encrypted database')
        store_group.add_argument('--ls', action='store_true', help='list all keys in the encrypted database')

        return parser.parse_args()

# helper functions

    def _read_local_file(self, file_name: str) -> str:

        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), file_name)) as file:
            return file.read().strip()

    def _setup_logging(self) -> None:

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s",
                            datefmt="%Y.%m.%d %H:%M:%S")


if __name__ == '__main__':
    Gluetube()
