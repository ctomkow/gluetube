# Craig Tomkow
# 2022-09-08

# local imports
from db import Store

# python imports
from abc import ABC, abstractmethod
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

        self.run()

    @abstractmethod
    def run(self) -> None:
        """The entry point of the pipeline. This will chain together your pipeline stages"""
        pass

    def stage1(method) -> Any:
        """Define the first stage of the pipeline."""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '1'
            return method(*args, **kwargs)

        return wrapper

    def stage2(method) -> Any:
        """Define the second stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '2'
            return method(*args, **kwargs)

        return wrapper

    def stage3(method) -> Any:
        """Define the third stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '3'
            return method(*args, **kwargs)

        return wrapper

    def stage4(method) -> Any:
        """Define the fourth stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '4'
            return method(*args, **kwargs)

        return wrapper

    def stage5(method) -> Any:
        """Define the fifth stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '5'
            return method(*args, **kwargs)

        return wrapper

    def stage6(method) -> Any:
        """Define the sixth stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '6'
            return method(*args, **kwargs)

        return wrapper

    def stage7(method) -> Any:
        """Define the seventh stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '7'
            return method(*args, **kwargs)

        return wrapper

    def stage8(method) -> Any:
        """Define the eighth stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '8'
            return method(*args, **kwargs)

        return wrapper

    def stage9(method) -> Any:
        """Define the ninth stage of the pipeline"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = '9'
            return method(*args, **kwargs)

        return wrapper
