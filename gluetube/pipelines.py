# Craig Tomkow
# 2022-09-08

# local imports
from db import Database

# python imports
from abc import ABC, abstractmethod
from typing import Any
import os


class Pipeline(ABC):

    def __init__(self, *variable_groups) -> None:

        # status is the outside view of the pipeline (e.g. from command.py point of view)
        # self.status = ['running', 'crashed', 'completed']

        # stage is inside view of pipeline (e.g. reporting from within via decorators)
        # self.stage = ['initializing', 'extracting', 'transforming', 'loading']

        # if 'running', the stage is ['initializing'|'extracting'|'transforming'|'loading']
        # if 'crashed', the last stage the pipeline was in will be shown
        # if 'completed', the last state of the pipeline was in will be shown (e.g. 'loading')

        # TODO: set pipeline stage='initializing', an sqlite3 call

        db_store = Database('store.db')

        for group in variable_groups:
            db_store.create_table(group)

            # injecting stored key:values as environment variables into the running pipeline process context
            # TODO: replace 'CONFL' with user provided grouping's that they want injected into the context
            key_values = db_store.all_key_values(group)
            for kv in key_values:
                os.environ[f"{group}_{kv[0]}"] = kv[1]

        # raise SystemExit

        self.run()

    @abstractmethod
    def run(self) -> None:
        """The entry point of the pipeline. This will chain together your _extract, _transform, _load methods"""
        pass

    def extract(method) -> Any:
        """Define the first stage of the pipeline. Extracting data from the source system"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = 'extracting'
            return method(*args, **kwargs)

        return wrapper

    def transform(method) -> Any:
        """Define the middle stage of the pipeline. Transform data for the target system"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = 'transforming'
            return method(*args, **kwargs)

        return wrapper

    def load(method) -> Any:
        """Define the final stage of the pipeline. Load the transformed data into the target system"""

        def wrapper(*args, **kwargs):
            # TODO: write stage to db as a separate call
            args[0].stage = 'loading'
            return method(*args, **kwargs)

        return wrapper
