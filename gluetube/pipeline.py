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

    def __init__(self, keyvalue_groups: list) -> None:

        db_store = Store('store.db')

        # injecting stored key:values as environment variables into the running pipeline process context
        for group in keyvalue_groups:
            key_values = db_store.all_key_values(group)
            for kv in key_values:
                os.environ[f"{group}_{kv[0]}"] = kv[1]

    def stage(stage=0, msg=''):
        """Define the stage of the pipeline with a custom message."""

        def inner_stage(method) -> Any:

            def wrapper(*args, **kwargs):

                util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_pipeline_run_stage', [int(os.environ['PIPELINE_RUN_ID']), stage, msg]))
                return method(*args, **kwargs)

            return wrapper
        return inner_stage

    def __enter__(self):

        return self

    def __exit__(self, type, value, traceback):

        return False
