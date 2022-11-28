# 2022-10-21
# Craig Tomkow

# local imports
from db import Pipeline
import exception
import util

# python imports
from pathlib import Path
import re
import random
from typing import List, Tuple


# TODO: error handling
class PipelineScanner:

    pipeline_dir = None
    db_name = None
    db_dir_path = None
    db = None

    def __init__(self, pipeline_dir_path: Path, socket_file: Path, db_dir: Path = Path('.'), db_name: str = 'gluetube.db') -> list:

        self.pipeline_dir = pipeline_dir_path
        if not self.pipeline_dir.exists():
            raise exception.AutodiscoveryError(f"pipeline directory, {self.pipeline_dir}, does not exist")

        self.socket_file = socket_file
        self.db_name = db_name
        self.db_dir_path = db_dir

    # steps
    #  1. get tuple (py_file, directory, timestamp) of all pipelines on filesystem
    #  2. get tuple (py_file, directory, timestamp) of all pipelines from database
    #  3. compare; generate list of pipelines to be deleted and to be added
    #  4. make RPC calls
    def scan(self) -> None:

        # must do this within the scan method not the constructor, otherwise the db obj gets created in another thread
        self.db = self._connect_to_db(self.db_name, self.db_dir_path)

        # a tuple (py_file, directory, py_file_timestamp), representing a complete pipeline
        pipeline_dirs = self._all_dirs(self.pipeline_dir)
        fs_pipelines_with_timestamp = self._enumerate_fs_pipelines(pipeline_dirs)
        # tuple (py_file, directory) representing a pipeline
        fs_pipelines_no_timestamp = [x[:2] for x in fs_pipelines_with_timestamp]

        # a tuple (py_file, directory, py_file_timestamp), representing a complete pipeline
        db_data = self.db.all_pipelines()
        db_pipelines_with_timestamp = self._enumerate_db_pipelines(db_data)
        # tuple (py_file, directory) representing a pipeline
        db_pipelines_no_timestamp = [x[:2] for x in db_pipelines_with_timestamp]

        # this consists of ompletely new pipelines on the file system
        missing_fs_pipelines = self._cmp_two_elems(fs_pipelines_no_timestamp, db_pipelines_no_timestamp)

        # this consists of pipelines in the db that don't exist on the file system anymore
        missing_db_pipelines = self._cmp_two_elems(db_pipelines_no_timestamp, fs_pipelines_no_timestamp)

        # ### Now make RPC Calls ###

        # add new pipeline (to scheduler and db)
        for pipeline in missing_fs_pipelines:
            msg = util.craft_rpc_msg('set_pipeline',
                                     [
                                        self._generate_unique_pipeline_name(self.db),
                                        pipeline[0],
                                        pipeline[1],
                                        0.0
                                     ])
            util.send_rpc_msg_to_daemon(msg, self.socket_file)

        # remove orphaned pipelines (from scheduler and db)
        for pipeline in missing_db_pipelines:
            pipeline_id = self.db.pipeline_id_from_tuple(pipeline[0], pipeline[1])
            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('delete_pipeline', [pipeline_id]), self.socket_file)

    def _connect_to_db(self, name: str, dir_path: Path) -> Pipeline:
        try:
            if name == 'memory':
                db = Pipeline(in_memory=True)
            else:
                db = Pipeline(db_path=Path(dir_path, name))
        except exception.dbError:
            raise

        return db

    def _all_dirs(self, current_dir: Path) -> List[Path]:

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

    def _all_py_files(self, current_dir: Path) -> List[Path]:

        files = [x for x in current_dir.iterdir() if x.is_file()]

        # remove all non .py files
        for file in files.copy():
            if not re.search(r"\.py$", file.name):
                files.remove(file)
            else:
                pass

        return files

    # list of tuples representing the pipelines (py_file, directory, py_file_timestamp)
    def _enumerate_fs_pipelines(self, pipeline_dirs: List[Path]) -> List[Tuple[str, str, float]]:

        tuples = []
        for dir in pipeline_dirs:
            py_files = self._all_py_files(dir)
            for py_file in py_files:
                tuples.append((py_file.name, dir.name, py_file.lstat().st_mtime))

        return tuples

    def _enumerate_db_pipelines(self,
                                pipeline_data: List[Tuple[int, str, str, str, float]]) -> List[Tuple[str, str, float]]:

        enum = []

        for pipeline in pipeline_data:
            enum.append((pipeline[2], pipeline[3], pipeline[4]))

        return enum

    def _cmp_two_elems(self, a: list, b: list, cmp_type: str = 'a_diff_b') -> list:

        if cmp_type == 'a_diff_b':
            return list(set(a) - (set(b)))
        elif cmp_type == 'b_diff_a':
            return list(set(b) - (set(a)))
        elif cmp_type == 'diff_both':
            return list(set(a).symmetric_difference(set(b)))
        elif cmp_type == 'same_both':
            return list(set(a).intersection(set(b)))
        else:
            return []

    # source: https://www.uibk.ac.at/anglistik/staff/herdina/kursunterlagen/mayhew_a_a_concise_dictionary_of_middle_englishbooksee.org.pdf
    def _random_middle_english_adjective_and_noun(self) -> str:

        adjectives = [
            'admod',  # humble, gentle
            'aht',  # worthy, valiant
            'brant',  # steep, high
            'bel',  # beautiful
            'calme',  # calm
            'cant',  # lively, brave, cheerful
            'drery',  # sad, dreary
            'dilitable',  # delightful
            'ender',  # latter
            'erly',  # early
        ]
        nouns = [
            'abbay',  # church
            'alemaunde',  # almond
            'banere',  # banner
            'beere',  # beer
            'camamelle',  # camomile
            'candel',  # candle
            'disour',  # minstrel
            'duk',  # duke
            'elf',  # elf
            'ey',  # egg
        ]

        adj = adjectives[random.randint(0, len(adjectives)-1)]
        noun = nouns[random.randint(0, len(nouns)-1)]

        return f"{adj}-{noun}"

    def _generate_unique_pipeline_name(self, db: Pipeline) -> str:

        name = self._random_middle_english_adjective_and_noun()
        tries = 1

        while db.pipeline_id_from_name(name):
            name = self._random_middle_english_adjective_and_noun()
            if tries >= 3:
                name = name + '_' + str(random.randint(0, 999))
                break
            tries += 1

        return name
