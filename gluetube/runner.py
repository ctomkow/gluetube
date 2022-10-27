# Craig Tomkow
# 2022-10-18

# local imports
import config
import util
import exceptions

# python imports
import logging
import subprocess
from subprocess import STDOUT, CalledProcessError
import sys
import os
from venv import EnvBuilder
from pathlib import Path


class Runner:

    def __init__(self, pipeline_id: int, pipeline_name: str, py_file_name: str, pipeline_dir_name: str) -> None:

        try:
            gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
            raise exceptions.RunnerError(f"Failed to initialize runner. {e}") from e

        self.base_dir = gt_cfg.pipeline_dir
        self.p_id = pipeline_id
        self.p_name = pipeline_name
        self.py_file = py_file_name
        self.p_dir = pipeline_dir_name

    def run(self) -> None:

        dir_abs_path = f"{self.base_dir}/{self.p_dir}"

        if not _venv_exists(f"{dir_abs_path}/.venv"):
            _create_venv(dir_abs_path)
            _symlink_gluetube_to_venv(f"{dir_abs_path}/.venv")

        # attempt to install pipeline requirements every time it is run
        # this is required because the pipeline could have changed along with it's requirements.txt
        if _requirements_exists(f"{dir_abs_path}/requirements.txt"):
            _install_pipeline_requirements(dir_abs_path)

        # modified environment variables of pipeline for gluetube system
        gluetube_env_vars = os.environ.copy()
        gluetube_env_vars['PIPELINE_ID'] = str(self.p_id)

        logging.info(f"Pipeline: {self.p_name}, started.")
        util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_status', [self.p_id, 'running']))

        try:
            # TODO: for DEV pipeline mode, print to logging, for normal operation, silence it.
            # for line in subprocess.check_output([".venv/bin/python", self.py_file], stderr=STDOUT, text=True, cwd=dir_abs_path, env=gluetube_env_vars).split('\n'):
            #     print(line)
            subprocess.check_output([".venv/bin/python", self.py_file], stderr=STDOUT, text=True, cwd=dir_abs_path, env=gluetube_env_vars)
        except CalledProcessError as e:
            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_status', [self.p_id, 'crashed']))
            util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_stacktrace', [self.p_id, e.output]))
            raise

        util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_status', [self.p_id, 'finished']))
        util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_stacktrace', [self.p_id, '']))
        logging.info(f"Pipeline: {self.p_name}, finished successfully.")

# helper functions


def _venv_exists(path: str) -> bool:

    path = Path(path)
    return path.is_dir()


def _requirements_exists(path: str) -> bool:
    path = Path(path)
    return path.is_file()


def _create_venv(dir: str) -> None:

    venv_abs_path = f"{dir}/.venv"
    venv = EnvBuilder(with_pip=True, symlinks=True)
    venv.ensure_directories(venv_abs_path)
    venv.create(venv_abs_path)


def _symlink_gluetube_to_venv(venv_dir: str) -> None:

    # 3.10
    py_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    src = f"{dir_path}"
    dst = f"{venv_dir}/lib/python{py_version}/site-packages/gluetube"
    os.symlink(src, dst)


def _install_pipeline_requirements(dir: str) -> None:

    try:
        subprocess.check_output(['.venv/bin/pip', 'install', '-r', 'requirements.txt'], cwd=dir)
    except CalledProcessError:
        raise
