# Craig Tomkow
# 2022-09-08

# local imports
from db import Store
import util
import exception

# python imports
from typing import Any, List
import os
from sqlite3 import OperationalError


def load_keyvalues(groups: List[str] = []) -> None:

    db_store = Store('store.db')

    # injecting stored key:values as environment variables into the running pipeline process context
    for group in groups:
        try:
            key_values = db_store.all_key_values(group)
        except OperationalError as e:
            raise exception.PipelineInitializationError() from e
        for kv in key_values:
            os.environ[f"{group}_{kv[0]}"] = kv[1]


def stage(stage=0, msg=''):
    """Define the stage of the pipeline with a custom message."""

    def inner_stage(method) -> Any:

        def wrapper(*args, **kwargs):

            util.send_rpc_msg_to_daemon(
                util.craft_rpc_msg('set_pipeline_run_stage_and_stage_msg',
                                   [int(os.environ['PIPELINE_RUN_ID']), stage, msg]))
            return method(*args, **kwargs)

        return wrapper
    return inner_stage
