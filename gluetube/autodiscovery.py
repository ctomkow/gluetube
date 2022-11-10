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

    # steps
    #  1. get tuple (py_file, directory, timestamp) of all pipelines on filesystem
    #  2. get tuple (py_file, directory, timestamp) of all pipelines from database
    #  3. compare; generate list of pipelines to be deleted and to be added
    #  4. make RPC calls
    def scan(self) -> None:

        # a tuple (py_file, directory, py_file_timestamp), representing a pipeline
        pipeline_dirs = self._all_dirs(self.pipeline_dir)
        fs_pipelines = self._enumerate_fs_pipelines(pipeline_dirs)

        # a tuple (py_file, directory, py_file_timestamp), representing a pipeline
        db_data = self._all_db_pipelines()
        db_pipelines = self._enumerate_db_pipelines(db_data)

        missing_fs_pipelines = self._diff_two_elems(fs_pipelines, db_pipelines)
        missing_db_pipelines = self._diff_two_elems(db_pipelines, fs_pipelines)

        # ### Now make RPC Calls ###

        # add new pipeline (to scheduler and db)
        for pipeline in missing_fs_pipelines:
            msg = util.craft_rpc_msg('set_new_pipeline',
                                     [
                                        re.split(r"\.py$", pipeline[0])[0],  # TODO: change this to add random name like docker
                                        pipeline[0],
                                        pipeline[1],
                                        pipeline[2]
                                     ])
            util.send_rpc_msg_to_daemon(msg)

        # remove orphaned pipelines (from scheduler and db)
        for pipeline in missing_db_pipelines:
            pipeline_id = self._db_pipeline_id(pipeline[0], pipeline[1])
            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('delete_pipeline', [pipeline_id]))

        # TODO: on new timestamp or new pipeline, if option set to run on discovery,
        #       use a 'set' RPC call to also have scheduler run right away

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

    # TODO: rework how db is read to allow for easy unit tests (e.g. allow test to specify :memory: db type for testing)
    def _all_db_pipelines(self) -> list[tuple[int, str, str, str, float, int]]:

        try:
            db = Pipeline('gluetube.db')
        except exception.dbError:
            raise

        pipelines = db.all_pipelines()
        db.close()

        return pipelines

    # TODO: rework how db is read to allow for easy unit tests (e.g. allow test to specify :memory: db type for testing)
    def _db_pipeline_id(self, py_name: str, dir_name: str) -> int:

        try:
            db = Pipeline('gluetube.db')
        except exception.dbError:
            raise

        pipeline_id = db.pipeline_id_from_tuple(py_name, dir_name)
        db.close()

        return pipeline_id

    # list of tuples representing the pipelines (py_file, directory, py_file_timestamp)
    def _enumerate_fs_pipelines(self, pipeline_dirs: list[Path]) -> list[tuple[str, str, float]]:

        tuples = []
        for dir in pipeline_dirs:
            py_files = self._all_py_files(dir)
            for py_file in py_files:
                tuples.append((py_file.name, dir.name, py_file.lstat().st_mtime))

        return tuples

    def _enumerate_db_pipelines(self,
                                pipeline_data: list[tuple[int, str, str, str, float, int]]) -> list[tuple[str, str, float]]:

        enum = []

        for pipeline in pipeline_data:
            enum.append((pipeline[2], pipeline[3], pipeline[4]))

        return enum

    def _diff_two_elems(self, a: list, b: list, diff_type: str = 'a_diff_b') -> list:

        if diff_type == 'a_diff_b':
            return list(set(a) - (set(b)))
        elif diff_type == 'b_diff_a':
            return list(set(b) - (set(a)))
        elif diff_type == 'diff_both':
            return list(set(a).symmetric_difference(set(b)))
        else:
            return []
