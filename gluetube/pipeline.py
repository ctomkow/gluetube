# Craig Tomkow
# 2022-09-08

# local imports
from db import Store
import util

# python imports
from abc import ABC
from typing import Any
import os


class Pipeline(ABC):

    def __init__(self, *variable_groups) -> None:

        # status is the outside view of the pipeline (e.g. from command.py point of view)
        # self.status = ['running', 'crashed', 'completed']

        # stage is inside view of pipeline (e.g. reporting from within via decorators)
        # self.stage = ['1', ... '9']

        # if 'running', the stage is ['1', ... '9']
        # if 'crashed', the last stage the pipeline was in will be shown
        # if 'completed', the last state of the pipeline was in will be shown (e.g. '4')

        # TODO: set pipeline stage='initializing', an sqlite3 call

        # TODO: check to see if PPID is still alive, if not, then exit (to stop the cron)

        db_store = Store('store.db')

        # injecting stored key:values as environment variables into the running pipeline process context
        for group in variable_groups:
            key_values = db_store.all_key_values(group)
            for kv in key_values:
                os.environ[f"{group}_{kv[0]}"] = kv[1]

    def stage(stage=0, msg=''):
        """Define the stage of the pipeline with a custom message."""

        def inner_stage(method) -> Any:

            def wrapper(*args, **kwargs):

                util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_stage', [int(os.environ['PIPELINE_ID']), stage, msg]))
                return method(*args, **kwargs)

            return wrapper
        return inner_stage
