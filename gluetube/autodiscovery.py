# 2022-10-21
# Craig Tomkow

# local imports
from db import Pipeline
import exception
import util

# python imports
from pathlib import Path
import re


# TODO: error handling
class PipelineScanner:

    pipeline_dir = None

    def __init__(self, pipeline_dir: str) -> list:

        self.pipeline_dir = Path(pipeline_dir)
        if not self.pipeline_dir.exists():
            raise exception.AutodiscoveryError(f"pipeline directory, {self.pipeline_dir}, does not exist")

    # TODO: clean up method, pull out parts into own helper methods for easy testing
    # steps
    #  1. get tuple (py_file, directory, timestamp) of all pipelines on filesystem
    #  2. get all existing pipelines from database
    #  3. compare; generate list of pipelines to be deleted and to be added
    #  4. make RPC calls
    def scan(self) -> None:

        # a tuple (py_file, directory, py_file_timestamp), representing a pipeline
        pipeline_dirs = self._all_dirs(self.pipeline_dir)
        enum_pipelines = self._enumerate_pipelines(pipeline_dirs)

        db_pipelines = self._all_db_pipelines()

        if not db_pipelines:
            self._compare_pipelines(enum_pipelines, (db_pipelines[2], db_pipelines[3], db_pipelines[4]))

        missing_pipeline_tuples = []
        valid_pipeline_ids = []

        # # compare (py_file, directory) pipelines found in directory with pipelines found in database
        # for tuple in pipeline_tuples:
        #     pipeline_found = False
        #     for pipeline in all_pipelines_from_db.copy():
        #         if (tuple[0] == pipeline[2]) and (tuple[1] == pipeline[3]):
        #             # pipeline exists in db
        #             pipeline_found = True
        #             break
        #     if pipeline_found:
        #         valid_pipeline_ids.append(pipeline[0])
        #     else:
        #         missing_pipeline_tuples.append(tuple)

        # now create list of pipeline id's that need to be deleted from db
        not_valid_pipeline_ids = []
        for pipeline in db_pipelines.copy():
            if pipeline[0] not in valid_pipeline_ids:
                not_valid_pipeline_ids.append(pipeline[0])

        # TODO: on new timestamp or new pipeline, if option set to run on discovery,
        #       use a 'set' RPC call to also have scheduler run right away

        # ### Now make RPC Calls ###

        # remove orphaned pipelines (from scheduler and db)
        for id in not_valid_pipeline_ids:
                            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('delete_pipeline', [id]))

        # add new pipeline (to scheduler and db)
        for pipeline in missing_pipeline_tuples:
            msg = util.craft_rpc_msg('set_new_pipeline',
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
            if (dir.name == 'None') or (re.search(r"^\.", dir.name)) or (re.search(r"^__", dir.name)):
                dirs.remove(dir)
            else:
                pass

        pipeline_dir_list = []
        for dir in dirs:
            pipeline_dir_list.append(dir.absolute())
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

    def _all_db_pipelines(self) -> list[tuple[int, str, str, str, float, int]]:

        try:
            db = Pipeline('gluetube.db')
        except exception.dbError:
            raise

        pipelines = db.all_pipelines()
        db.close()

        return pipelines

    # list of tuples representing the pipelines (py_file, directory, py_file_timestamp)
    def _enumerate_pipelines(self, pipeline_dirs: list[Path]) -> list[tuple[str, str, float]]:

        tuples = []
        for dir in pipeline_dirs:
            py_files = self._all_py_files(dir)
            for py_file in py_files:
                tuples.append((py_file.name, dir.name, py_file.lstat().st_mtime))

        return tuples

    def _compare_pipelines(self,
                           enumerated: list[tuple[str, str, float]],
                           stored: list[tuple[str, str, float]]) -> tuple[list, list]:

        missing_pipeline_tuples = []
        valid_pipeline_ids = []

        for tuple in enumerated:
            pipeline_found = False
            for pipeline in stored.copy():
                if (tuple[0] == pipeline[2]) and (tuple[1] == pipeline[3]):
                    # pipeline exists in db
                    pipeline_found = True
                    break
            if pipeline_found:
                valid_pipeline_ids.append(pipeline[0])
            else:
                missing_pipeline_tuples.append(tuple)

        return None  # TODO: change this
