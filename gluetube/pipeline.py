# Craig Tomkow
# 2022-09-08

# local imports
import util

# python imports
from typing import Any
import os
from pathlib import Path

cron = None
at = None


def stage(stage=0, msg=''):
    """Define the stage of the pipeline with a custom message."""

    def inner_stage(method) -> Any:

        def wrapper(*args, **kwargs):

            util.send_rpc_msg_to_daemon(
                util.craft_rpc_msg('set_pipeline_run_stage_and_stage_msg', [int(os.environ['PIPELINE_RUN_ID']), stage, msg]),
                Path(os.environ['SOCKET_FILE'])
            )
            return method(*args, **kwargs)

        return wrapper
    return inner_stage
