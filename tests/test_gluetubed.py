# Craig Tomkow
# 2022-12-13

# local imports
from gluetube.gluetubed import GluetubeDaemon
from gluetube.db import Pipeline, Store
from exception import DaemonError
import gluetube.config
from gluetube import util
from gluetube.runner import Runner

# python imports
from pathlib import Path
import os
import socket
from typing import Any, Dict
from datetime import datetime

# 3rd party imports
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
import pytest


class TestGluetubeDaemon:

    @pytest.fixture
    def abspath_test_tmp_dir(self) -> Path:

        return Path(os.path.dirname(os.path.realpath(__file__)), 'tmp')

    @pytest.fixture
    def sock(self, abspath_test_tmp_dir) -> socket.socket:

        return GluetubeDaemon()._setup_listener_unix_socket(Path(abspath_test_tmp_dir, 'gluetube.sock'))

    @pytest.fixture
    def scheduler(self, gt_cfg) -> BackgroundScheduler:

        scheduler = GluetubeDaemon()._setup_scheduler(101)
        runner = Runner(1, 'test', 'test.py', 'test_dir', 1, gt_cfg)
        scheduler.add_job(runner.run, trigger=DateTrigger(datetime(2999, 1, 1)), id=str(1))
        return scheduler

    @ pytest.fixture
    def db_p(self) -> Pipeline:

        db = Pipeline(in_memory=True)
        db.create_schema()
        db.insert_pipeline('test', 'test.py', 'test_dir', 'null')
        db.insert_pipeline_schedule(1)
        db.insert_pipeline_run(1, 1, 'running', '2022:01:01 00:00:00')
        return db

    @pytest.fixture
    def db_s(self) -> Store:

        db = Store('PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=', in_memory=True)
        db.create_table('common')
        db.insert_key_value('common', 'TEST', 'SECRET')
        return db

    @pytest.fixture
    def gt_cfg(self) -> gluetube.config.Gluetube:

        gt_cfg = gluetube.config.Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'gluetube.cfg').resolve().as_posix())
        gt_cfg.parse()
        return gt_cfg

    @pytest.fixture
    def kwargs(self, scheduler, db_p, db_s, gt_cfg) -> Dict[str, Any]:

        return {'scheduler': scheduler, 'db_p': db_p, 'db_s': db_s, 'gt_cfg': gt_cfg}

    def test_write_pid(self, abspath_test_tmp_dir) -> None:

        GluetubeDaemon()._write_pid(Path(abspath_test_tmp_dir, 'gluetube.pid'))
        assert Path(abspath_test_tmp_dir, 'gluetube.pid').exists()
        Path(abspath_test_tmp_dir, 'gluetube.pid').unlink()

    def test_write_pid_verify_pid(self, abspath_test_tmp_dir) -> None:

        GluetubeDaemon()._write_pid(Path(abspath_test_tmp_dir, 'gluetube.pid'))
        assert Path(abspath_test_tmp_dir, 'gluetube.pid').read_text() == str(os.getpid())
        Path(abspath_test_tmp_dir, 'gluetube.pid').unlink()

    def test_write_pid_already_exists(self, abspath_test_tmp_dir) -> None:

        Path(abspath_test_tmp_dir, 'gluetube.pid').write_text('99999')
        GluetubeDaemon()._write_pid(Path(abspath_test_tmp_dir, 'gluetube.pid'))
        assert Path(abspath_test_tmp_dir, 'gluetube.pid').read_text() == str(os.getpid())
        Path(abspath_test_tmp_dir, 'gluetube.pid').unlink()

    def test_setup_listener_unix_socket(self, abspath_test_tmp_dir) -> None:

        sock = GluetubeDaemon()._setup_listener_unix_socket(Path(abspath_test_tmp_dir, 'gluetube.sock'))
        assert isinstance(sock, socket.socket)
        Path(abspath_test_tmp_dir, 'gluetube.sock').unlink()

    def test_setup_scheduler(self) -> None:

        scheduler = GluetubeDaemon()._setup_scheduler(101)
        assert isinstance(scheduler, BackgroundScheduler)

    def test_setup_scheduler_zero_threads(self) -> None:

        with pytest.raises(DaemonError):
            GluetubeDaemon()._setup_scheduler(0)

    def test_setup_scheduler_million_threads(self) -> None:

        scheduler = GluetubeDaemon()._setup_scheduler(1000000)
        assert isinstance(scheduler, BackgroundScheduler)

    def test_recv_all(self, sock, abspath_test_tmp_dir) -> None:

        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(Path(abspath_test_tmp_dir, 'gluetube.sock').resolve().as_posix())
        client.sendall('asdfgh'.encode())

        conn, _ = sock.accept()
        rcv_data = GluetubeDaemon()._recvall(conn, 4)

        assert rcv_data.decode() == 'asdf'

    def test_schedule_pipelines(self, scheduler, db_p, gt_cfg) -> None:

        GluetubeDaemon()._schedule_pipelines(scheduler, db_p, gt_cfg)
        assert scheduler.get_job('1')

    def test_schedule_auto_discovery(self, scheduler, gt_cfg) -> None:

        gt_cfg.pipeline_dir = Path(Path(__file__).parent.resolve(), 'pipeline_dir').resolve().as_posix()
        gt_cfg.sqlite_app_name = 'memory'
        GluetubeDaemon()._schedule_auto_discovery(scheduler, gt_cfg)
        assert scheduler.get_job('pipeline_scanner')

    def test_set_pipeline(self, kwargs) -> None:

        GluetubeDaemon().set_pipeline('new_test', 'new_test.py', 'new_test_dir', '0', **kwargs)
        assert kwargs['scheduler'].get_job('2') and kwargs['db_p'].pipeline_id_from_name('new_test') == 2

    def test_delete_pipeline(self, kwargs) -> None:

        GluetubeDaemon().delete_pipeline(1, **kwargs)
        assert kwargs['scheduler'].get_job('1') is None and kwargs['db_p'].pipeline_id_from_name('test') is None

    def test_set_schedule(self, kwargs) -> None:

        GluetubeDaemon().set_schedule(1, **kwargs)
        assert kwargs['scheduler'].get_job('2') and kwargs['db_p'].pipeline_schedule(1, 2)

    def test_set_schedule_cron(self, kwargs) -> None:

        GluetubeDaemon().set_schedule_cron(1, '* * * * *', **kwargs)
        assert kwargs['scheduler'].get_job('1') and kwargs['db_p'].pipeline_schedule(1, 1)[5] == '* * * * *'

    def test_set_schedule_at(self, kwargs) -> None:

        GluetubeDaemon().set_schedule_at(1, '2099-01-01 00:00:00', **kwargs)
        assert kwargs['scheduler'].get_job('1') and kwargs['db_p'].pipeline_schedule(1, 1)[6] == '2099-01-01 00:00:00'

    def test_set_schedule_now(self, kwargs) -> None:

        GluetubeDaemon().set_schedule_now(1, **kwargs)
        assert kwargs['scheduler'].get_job('1') \
            and kwargs['db_p'].pipeline_schedule(1, 1)[5] == '' \
            and kwargs['db_p'].pipeline_schedule(1, 1)[6] == ''

    def test_delete_pipeline_schedule(self, kwargs) -> None:

        GluetubeDaemon().delete_schedule(1, **kwargs)
        assert kwargs['scheduler'].get_job('1') is None and kwargs['db_p'].pipeline_schedule(1, 1) is None

    def test_set_schedule_latest_run(self, kwargs) -> None:

        GluetubeDaemon().set_schedule_latest_run(1, 1, **kwargs)
        assert kwargs['db_p'].pipeline_schedule(1, 1)[8] == 1

    def test_set_pipeline_run(self, kwargs) -> None:

        GluetubeDaemon().set_pipeline_run(1, 1, 'what status?', 'now', **kwargs)
        assert kwargs['db_p'].pipeline_run(2)

    def test_set_pipeline_run_status(self, kwargs) -> None:

        GluetubeDaemon().set_pipeline_run_status(1, 'finished', **kwargs)
        assert kwargs['db_p'].pipeline_run(1)[2] == 'finished'

    def test_set_pipeline_run_stage_and_stage_msg(self, kwargs) -> None:

        GluetubeDaemon().set_pipeline_run_stage_and_stage_msg(1, 5, 'im here', **kwargs)
        assert kwargs['db_p'].pipeline_run(1)[3] == 5 and kwargs['db_p'].pipeline_run(1)[4] == 'im here'

    def test_set_pipeline_run_finished(self, kwargs) -> None:

        GluetubeDaemon().set_pipeline_run_finished(1, 'finished', '', '2022:01:01 00:00:00', **kwargs)
        assert kwargs['db_p'].pipeline_run(1)[2] == 'finished' and kwargs['db_p'].pipeline_run(1)[7] == '2022:01:01 00:00:00'

    def test_set_key_value(self, kwargs) -> None:

        GluetubeDaemon().set_key_value('MY_KEY', 'secret', **kwargs)
        assert kwargs['db_s'].all_key_values('common')[1][0] == 'MY_KEY' and util.decrypt(kwargs['db_s'].all_key_values('common')[1][1], 'PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=') == 'secret'

    def test_delete_key(self, kwargs) -> None:

        GluetubeDaemon().delete_key('TEST', **kwargs)
        assert kwargs['db_s'].value('common', 'TEST') is None
