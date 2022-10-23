# 2022-10-21
# Craig Tomkow

# local imports
from db import Pipeline
import exceptions

# python imports
from pathlib import Path
import re


# TODO: error handling
class PipelineScanner:

    pipeline_dir = None

    def __init__(self, pipeline_dir: str) -> list:

        self.pipeline_dir = Path(pipeline_dir)

    def scan(self) -> None:

        # a list of tuples, (directory, py_file), this uniquely identifies a pipeline
        pipeline_tuples = []

        pipeline_dirs = self._all_dirs(self.pipeline_dir)

        for dir in pipeline_dirs:
            py_files = self._all_py_files(dir)
            for py_file in py_files:
                pipeline_tuples.append((dir.name, py_file))

        db_pipelines = self._db_pipelines()

        orphan_pipeline_ids = []
        for pipeline in db_pipelines:
            for tuple in pipeline_tuples:
                if (pipeline[3] == tuple[0]) and (pipeline[2] == tuple[1]):
                    pass  # pipeline tuple exists in database!

        # TODO: finish determining differences between pipeline files tuple and what's in the database

        # TODO: MAYBE split off the (directory, py_file) tuple into it's own database, with id as foreign key to pipeline table????
        print(pipeline_tuples)
        # TODO: First, query db for all
        # TODO: make RPC call to daemon to update database and scheduler

    def _all_dirs(self, current_dir: Path) -> list:

        dirs = [x for x in current_dir.iterdir() if x.is_dir()]

        # remove 'None' dir or hidden dir
        for dir in dirs.copy():
            if (dir.name == 'None') or (re.search(r"^\.", dir.name)):
                dirs.remove(dir)
            else:
                pass

        pipeline_dir_list = []
        for dir in dirs:
            pipeline_dir_list.append(dir)
        return pipeline_dir_list

    def _all_py_files(self, current_dir: Path) -> list:

        files = [x for x in current_dir.iterdir() if x.is_file()]

        # remove all non .py files
        for file in files.copy():
            if not re.search(r"\.py$", file.name):
                files.remove(file)
            else:
                pass

        pipeline_py_list = []
        for file in files:
            pipeline_py_list.append(file.name)
        return pipeline_py_list

    def _db_pipelines(self) -> list:

        try:
            db = Pipeline('gluetube.db')
        except exceptions.dbError:
            raise

        pipelines = db.all_pipelines_details()
        db.close()

        return pipelines
