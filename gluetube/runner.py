# Craig Tomkow
# 2022-10-18

# local imports
import util
import exception
from db import Pipeline, Store
import config

# python imports
import logging
import subprocess
from subprocess import STDOUT, CalledProcessError
import sys
import os
from venv import EnvBuilder
from pathlib import Path
import datetime
from time import sleep
from typing import Set

# 3rd party imports
from jinja2 import Template, FileSystemLoader, Environment, meta


class Runner:

    def __init__(self, pipeline_id: int, pipeline_name: str, py_file_name: str, pipeline_dir_name: str, schedule_id: int, gt_cfg: config.Gluetube) -> None:

        self.base_dir = gt_cfg.pipeline_dir
        self.p_id = pipeline_id
        self.p_name = pipeline_name
        self.py_file = py_file_name
        self.p_dir = pipeline_dir_name
        self.s_id = schedule_id
        self.db_dir = gt_cfg.sqlite_dir
        self.db_app_name = gt_cfg.sqlite_app_name
        self.db_kv_name = gt_cfg.sqlite_kv_name
        self.db_kv_token = gt_cfg.sqlite_token
        self.socket_file = Path(gt_cfg.socket_file)
        self.http_proxy = gt_cfg.http_proxy
        self.https_proxy = gt_cfg.https_proxy

    def run(self) -> None:

        dir_abs_path = Path(Path(self.base_dir).resolve() / self.p_dir).resolve().as_posix()

        if not _venv_exists(f"{dir_abs_path}/.venv"):
            _create_venv(dir_abs_path)
            _symlink_gluetube_to_venv(f"{dir_abs_path}/.venv")

        # attempt to install pipeline requirements every time it is run
        # this is required because the pipeline could have changed along with it's requirements.txt
        if _requirements_exists(f"{dir_abs_path}/requirements.txt"):
            _install_pipeline_requirements(dir_abs_path, self.http_proxy, self.https_proxy)

        # ### THE 'START' of the pipeline ###

        # substitute variables in pipeline with database elements and write new tmp pipeline py file
        env = _load_template_env(Path(dir_abs_path))
        template = _jinja_template(env, self.py_file)
        variables = _all_variables_in_template(env, Path(dir_abs_path), self.py_file)
        db_kv = Store(self.db_kv_token, db_path=Path(self.db_dir, self.db_kv_name))
        pairs = _variable_value_pairs_for_template(variables, db_kv)
        pipeline_as_a_string = template.render(pairs)

        # get current time and create a new db entry for current run
        start_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logging.info(f"Pipeline: {self.p_name}, started.")
        util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_pipeline_run', [self.p_id, self.s_id, 'running', start_time]), self.socket_file)

        sleep(1)  # avoid race condition on db lookup, a hack i know TODO: fix

        # get pipeline_run_id, also set the current_run of pipeline to the pipeline_run_id
        db = Pipeline(db_path=Path(self.db_dir, self.db_app_name))
        pipeline_run_id = db.pipeline_run_id_by_pipeline_id_and_start_time(self.p_id, start_time)
        util.send_rpc_msg_to_daemon(util.craft_rpc_msg('set_schedule_latest_run', [self.s_id, pipeline_run_id]), self.socket_file)

        # modified environment variables of pipeline for gluetube system
        gluetube_env_vars = os.environ.copy()
        gluetube_env_vars['PIPELINE_RUN_ID'] = str(pipeline_run_id)
        gluetube_env_vars['SOCKET_FILE'] = self.socket_file.resolve().as_posix()

        # Finally, actually fork the pipeline process
        try:
            subprocess.check_output(
                [".venv/bin/python3", "-"],
                text=True, cwd=dir_abs_path,
                env=gluetube_env_vars,
                input=pipeline_as_a_string,
                stderr=STDOUT
            )
        except CalledProcessError as e:
            util.send_rpc_msg_to_daemon(
                util.craft_rpc_msg(
                    'set_pipeline_run_finished',
                    [pipeline_run_id, 'crashed', e.output, datetime.datetime.now(datetime.timezone.utc).isoformat()]
                ),
                self.socket_file
            )
            raise exception.RunnerError(f'Pipeline {self.p_name} crashed.') from None  # don't leak things

        util.send_rpc_msg_to_daemon(
            util.craft_rpc_msg(
                'set_pipeline_run_finished',
                [pipeline_run_id, 'finished', '', datetime.datetime.now(datetime.timezone.utc).isoformat()]
            ),
            self.socket_file
        )
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


def _install_pipeline_requirements(dir: str, http_proxy: str = '', https_proxy: str = '') -> None:

    env_vars = os.environ.copy()
    env_vars['HTTP_PROXY'] = http_proxy
    env_vars['HTTPS_PROXY'] = https_proxy

    try:
        subprocess.check_output(['.venv/bin/pip3', 'install', '-r', 'requirements.txt'], cwd=dir, env=env_vars)
    except CalledProcessError:
        raise


def _load_template_env(directory: Path) -> Environment:

    file_loader = FileSystemLoader(directory.resolve().as_posix())
    env = Environment(loader=file_loader)
    return env


def _jinja_template(env: Environment, file: str) -> Template:

    template = env.get_template(file)
    return template


def _all_variables_in_template(env: Environment, directory: Path, file: str) -> Set[str]:

    data = Path(directory.resolve(), file).read_text()
    ast = env.parse(data)
    variables = meta.find_undeclared_variables(ast)
    return variables


def _variable_value_pairs_for_template(variables: Set[str], db: Store) -> dict:

    pairs = {}
    for var in variables:
        value = db.value('common', var)
        if not value:
            continue
        pairs[var] = f"'{value}'"

    return pairs
