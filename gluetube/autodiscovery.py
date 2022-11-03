# 2022-10-21
# Craig Tomkow

# local imports
from db import Pipeline
import exceptions
import util

# python imports
from pathlib import Path
import re
import importlib
import sys


# TODO: error handling
class PipelineScanner:

    pipeline_dir = None

    def __init__(self, pipeline_dir: str) -> list:

        self.pipeline_dir = Path(pipeline_dir)

    # TODO: clean up method, pull out parts into own helper methods for easy testing
    # steps
    #  1. get tuple (py_file, directory, timestamp) of all pipelines on filesystem
    #  2. get all existing pipelines from database
    #  3. compare; generate list of pipelines to be deleted and to be added
    #  4. make RPC calls
    def scan(self) -> None:

        # a list of tuples, (py_file, directory, timestamp)
        pipeline_tuples = []
        pipeline_dirs = self._all_dirs(self.pipeline_dir)

        # generate the list of (py_file, directory, timestamp) unique tuples from pipeline directory
        for dir in pipeline_dirs:

            # TODO: need to be able to remove from path if directory disappears
            # to be able to import modules later
            #sys.path.append(dir.as_posix())

            py_files = self._all_py_files(dir)
            for py_file in py_files:
                pipeline_tuples.append((py_file.name, dir.name, py_file.lstat().st_mtime))

        db_pipelines = self._db_pipelines()

        missing_pipeline_tuples = []
        valid_pipeline_ids = []

        # compare pipelines found in directory with pipelines found in database
        for tuple in pipeline_tuples:
            pipeline_found = False
            for pipeline in db_pipelines.copy():
                if (tuple[0] == pipeline[2]) and (tuple[1] == pipeline[3]):
                    # pipeline exists in db
                    pipeline_found = True
                    break
            if pipeline_found:
                valid_pipeline_ids.append(pipeline[0])
            else:
                missing_pipeline_tuples.append(tuple)

        # now create list of pipeline id's that need to be deleted from db
        not_valid_pipeline_ids = []
        for pipeline in db_pipelines.copy():
            if pipeline[0] not in valid_pipeline_ids:
                not_valid_pipeline_ids.append(pipeline[0])

        # ### Now make RPC Calls ###

        # orphaned pipelines that need to be removed from db
        for id in not_valid_pipeline_ids:
            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('delete_pipeline', [id]))

        # TODO: continue on with the import module stuff, ehhhh it's not as easy as it looks
        # found new pipelines (py_file, directory, timestamp) tuples in pipeline directory
        for pipeline in missing_pipeline_tuples:
            #module = importlib.import_module(re.split(r"\.py$", pipeline[0])[0])
            msg = util.craft_rpc_msg('set_pipeline',
                                     [
                                        re.split(r"\.py$", pipeline[0])[0],
                                        pipeline[0],
                                        pipeline[1],
                                        pipeline[2]
                                     ])
            util.send_rpc_msg_to_daemon(msg)

    def _all_dirs(self, current_dir: Path) -> list[Path]:

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

    def _all_py_files(self, current_dir: Path) -> list[Path]:

        files = [x for x in current_dir.iterdir() if x.is_file()]

        # remove all non .py files
        for file in files.copy():
            if not re.search(r"\.py$", file.name):
                files.remove(file)
            else:
                pass

        return files

    def _db_pipelines(self) -> list:

        try:
            db = Pipeline('gluetube.db')
        except exceptions.dbError:
            raise

        pipelines = db.all_pipelines()
        db.close()

        return pipelines
