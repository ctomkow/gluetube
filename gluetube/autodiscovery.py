# 2022-10-21
# Craig Tomkow

# local imports
from db import Pipeline
import exceptions
import config
import util

# python imports
from pathlib import Path
import re
import struct
import json
import socket


# TODO: error handling
class PipelineScanner:

    pipeline_dir = None

    def __init__(self, pipeline_dir: str) -> list:

        self.pipeline_dir = Path(pipeline_dir)

    # TODO: clean up method, pull out parts into own helper methods for easy testing
    def scan(self) -> None:

        # a list of tuples, (py_file, directory), this uniquely identifies a pipeline
        pipeline_tuples = []
        pipeline_dirs = self._all_dirs(self.pipeline_dir)

        # generate the list of (py_file, directory) unique tuples from pipeline directory
        for dir in pipeline_dirs:
            py_files = self._all_py_files(dir)
            for py_file in py_files:
                pipeline_tuples.append((py_file, dir.name))

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

        not_valid_pipeline_ids = []

        # now create list of pipeline id's that need to be deleted from db
        for pipeline in db_pipelines.copy():
            if pipeline[0] not in valid_pipeline_ids:
                not_valid_pipeline_ids.append(pipeline[0])

        # ### Now make RPC Calls ###

        # orphaned pipelines that need to be removed from db
        for id in not_valid_pipeline_ids:
            self._send_rpc_msg_to_daemon(self._craft_rpc_msg('delete_pipeline', [id]))

        # found new pipelines (py_file, directory) tuples in pipeline directory
        for pipeline in missing_pipeline_tuples:
            self._send_rpc_msg_to_daemon(self._craft_rpc_msg('set_pipeline', [re.split(r"\.py$", pipeline[0])[0], pipeline[0], pipeline[1], '']))

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

    # TODO: move this to the utils module
    def _craft_rpc_msg(self, func: str, params: list) -> bytes:

        msg_dict = {'function': func, 'parameters': params}
        msg_str = json.dumps(msg_dict)
        msg_bytes = str.encode(msg_str)
        return struct.pack('>I', len(msg_bytes)) + msg_bytes

    # TODO: move this to the utils module
    def _send_rpc_msg_to_daemon(self, msg: bytes) -> None:

        try:
            gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
            raise exceptions.rpcError(f"RPC call failed. {e}") from e

        server_address = gt_cfg.socket_file
        if not Path(gt_cfg.socket_file).exists():
            raise exceptions.rpcError(f"Unix domain socket, {gt_cfg.socket_file}, not found")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(server_address)
            sock.sendall(msg)
        except ConnectionRefusedError as e:
            raise exceptions.rpcError(f"RPC call failed. {e}") from e