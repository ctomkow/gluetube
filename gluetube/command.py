# Craig Tomkow
# 2022-09-09

# local imports
from db import Pipeline, Store
import config
import util
from gluetubed import GTdaemon
from runner import Runner

# python imports
from pathlib import Path


def init_gluetube() -> None:

    gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
    Path(gt_cfg.pipeline_dir).mkdir(exist_ok=True)
    Path(gt_cfg.database_dir).mkdir(exist_ok=True)
    db = Pipeline('gluetube.db')
    db.create_schema()
    db = Store('store.db')
    print('setup complete.')


def ls_pipelines() -> list:

    db = Pipeline('gluetube.db')
    return db.all_pipelines()


def run_pipeline(name: str) -> None:

    db = Pipeline('gluetube.db')
    pipeline_py = db.pipeline_py_name(name)[0]
    pipeline_dir = db.pipeline_dir_name(name)[0]

    Runner(name, pipeline_py, pipeline_dir).run()


def start_daemon() -> None:

    GTdaemon().start()


def start_daemon_fg() -> None:

    GTdaemon().start(fg=True)
