# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store

# python imports
import logging
import subprocess
from subprocess import CalledProcessError
import sys
import os
from venv import EnvBuilder
from pathlib import Path


def init_gluetube() -> None:

    db = Pipeline('gluetube.db')
    db.create_schema()
    db = Store('store.db')


def ls_pipelines() -> list:

    db = Pipeline('gluetube.db')
    return db.all_pipelines()


def run_pipeline(name: str, conf_dir: list, pipeline_locations: list) -> None:

    # TODO: set pipeline status='running', an sqlite3 call

    db = Pipeline('gluetube.db')
    pipeline_file = db.pipeline_py_name(name)[0]
    pipeline_dir = db.pipeline_dir_name(name)[0]

    abs_path_to_pipeline_dir = _append_conf_name_to_dir(pipeline_dir, pipeline_locations)[0]
    pipeline_abs_path = _append_conf_name_to_dir(f"{pipeline_dir}/{pipeline_file}", pipeline_locations)[0]

    if not _venv_exists(f"{abs_path_to_pipeline_dir}/.venv"):
        _create_venv(abs_path_to_pipeline_dir)
        _symlink_gluetube_to_venv(f"{abs_path_to_pipeline_dir}/.venv")

    # attempt to install pipeline requirements every time it is run
    # this is required because the pipeline could have changed along with it's requirements.txt
    if _requirements_exists(f"{abs_path_to_pipeline_dir}/requirements.txt"):
        _install_pipeline_requirements(abs_path_to_pipeline_dir)

    try:
        subprocess.check_output([f"{abs_path_to_pipeline_dir}/.venv/bin/python", pipeline_abs_path])
    except CalledProcessError:
        # TODO: set pipeline status='crashed', an sqlite3 call
        raise

    # TODO: set pipeline status='completed', an sqlite3 call
    logging.info(f"Pipeline: {name} completed.")

# helper functions


def _append_conf_name_to_dir(conf_name: str, conf_dir: list) -> list:

    return [s + conf_name for s in conf_dir]


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
    # TODO: fix absolute path in src
    src = f"/home/gluetube/.local/lib/python{py_version}/site-packages/gluetube"
    dst = f"{venv_dir}/lib/python{py_version}/site-packages/gluetube"
    os.symlink(src, dst)


def _install_pipeline_requirements(abs_path_to_pipeline_dir: str) -> None:

    try:
        subprocess.check_output([f"{abs_path_to_pipeline_dir}/.venv/bin/pip", 'install', '-r', f"{abs_path_to_pipeline_dir}/requirements.txt"])
    except CalledProcessError:
        raise
