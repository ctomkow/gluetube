# Craig Tomkow
# 2022-11-14

# local imports
from gluetube.db import Store, Pipeline
from gluetube import util
from exception import dbError

# 3rd party imports
import pytest


class TestStore:

    @pytest.fixture
    def db(self) -> Store:

        return Store('PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=', in_memory=True)

    def test_create_table(self, db) -> None:

        db.create_table('TABLEA')
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='TABLEA';"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 'TABLEA'
        db.close()

    def test_create_table_existing_table(self, db) -> None:

        db.create_table('TABLEA')
        db.create_table('TABLEA')
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='TABLEA';"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 'TABLEA'
        db.close()

    def test_insert_key_value(self, db) -> None:

        db.create_table('TABLEA')
        db.insert_key_value('TABLEA', 'user_bob', 'pass_asdf')

        query = "SELECT value FROM TABLEA WHERE key='user_bob';"
        results = db._conn.cursor().execute(query)

        assert util.decrypt(results.fetchone()[0], 'PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=') == 'pass_asdf'
        db.close()

    def test_insert_key_empty_value(self, db) -> None:

        db.create_table('TABLEA')
        db.insert_key_value('TABLEA', 'user_bob', '')

        query = "SELECT value FROM TABLEA WHERE key='user_bob';"
        results = db._conn.cursor().execute(query)

        assert util.decrypt(results.fetchone()[0], 'PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=') == ''
        db.close()

    def test_insert_key_empty_key(self, db) -> None:

        db.create_table('TABLEA')
        with pytest.raises(dbError):
            db.insert_key_value('TABLEA', '', 'password')
        db.close()

    def test_all_key_values(self, db) -> None:

        db.create_table('TABLEA')
        db.insert_key_value('TABLEA', 'user_bob', 'pass_asdf')
        results = db.all_key_values('TABLEA')

        assert results[0][0] == 'user_bob' and util.decrypt(results[0][1], 'PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=') == 'pass_asdf'
        db.close()

    def test_all_key_values_empty_table(self, db) -> None:

        db.create_table('TABLEA')
        results = db.all_key_values('TABLEA')

        assert results == []
        db.close()

    def test_all_key_values_missing_table(self, db) -> None:

        with pytest.raises(dbError):
            db.all_key_values('TABLEA')
        db.close()


class TestPipeline:

    @pytest.fixture
    def db(self) -> Pipeline:

        return Pipeline(in_memory=True)

    @pytest.fixture
    def pipeline(self, db) -> None:

        db.create_schema()
        db.insert_pipeline('test', 'test.py', 'test_dir', '111.1')

    @pytest.fixture
    def pipeline_name(self, db) -> None:

        db.create_schema()
        db.insert_pipeline('test-name', 'test.py', 'test_dir', '111.1')

    @pytest.fixture
    def schedule_cron(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_schedule(1, '* * * * *', '', 0, 0, 0, 0)

    @pytest.fixture
    def schedule_at(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_schedule(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)

    @pytest.fixture
    def run(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_run(1, 1, 'running', '2023-01-01 00:00:00')

    def test_create_schema(self, db) -> None:

        db.create_schema()
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [('pipeline',), ('pipeline_schedule',), ('pipeline_run',)]
        db.close()

    def test_create_schema_tables_exist_with_data(self, db, pipeline) -> None:

        db.create_schema()
        query = "SELECT name, py_name, dir_name, py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('test', 'test.py', 'test_dir', 111.1)
        db.close()

    # ##### PIPELINE TABLE TESTS ##### #

    def test_insert_pipeline(self, db, pipeline) -> None:

        query = "SELECT name, py_name, dir_name, py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('test', 'test.py', 'test_dir', 111.1)
        db.close()

    def test_insert_pipeline_return_id(self, db) -> None:

        db.create_schema()
        rowid = db.insert_pipeline('test', 'test.py', 'test_dir', '111.1')

        assert rowid == 1

    def test_insert_pipeline_empty_data(self, db) -> None:

        db.create_schema()

        with pytest.raises(dbError):
            db.insert_pipeline('', '', '', '')
        db.close()

    def test_insert_pipeline_duplicate_name(self, db, pipeline) -> None:

        with pytest.raises(dbError):
            db.insert_pipeline('test', 'test1.py', 'test_dir1', '222.2')
        db.close()

    def test_delete_pipeline(self, db, pipeline) -> None:

        db.delete_pipeline(1)

        query = "SELECT id from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() is None
        db.close()

    def test_delete_pipeline_empty_data(self, db) -> None:

        db.create_schema()
        db.delete_pipeline(1)

        assert True
        db.close()

    def test_update_pipeline_name(self, db, pipeline) -> None:

        db.update_pipeline_name(1, 'new-name')
        query = "SELECT name from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 'new-name'

    def test_update_pipeline_name_wrong_id(self, db, pipeline) -> None:

        db.update_pipeline_name(2, 'new-name')
        query = "SELECT name from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 'test'

    def test_update_pipeline_py_timestamp(self, db, pipeline) -> None:

        db.update_pipeline_py_timestamp(1, '222.2')
        query = "SELECT py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 222.2

    def test_update_pipeline_py_timestamp_wrong_id(self, db, pipeline) -> None:

        db.update_pipeline_py_timestamp(2, '222.2')
        query = "SELECT py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 111.1

    # ##### PIPELINE SCHEDULE TABLE TESTS ##### #

    def test_delete_pipeline_schedule(self, db, pipeline, schedule_cron) -> None:

        db.delete_pipeline_schedule(1)

        query = "SELECT id from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() is None
        db.close()

    def test_delete_pipeline_schedule_empty_data(self, db, pipeline) -> None:

        db.delete_pipeline_schedule(1)

        assert True
        db.close()

    def test_insert_pipeline_schedule_return_id(self, db, pipeline) -> None:

        db.create_schema()
        rowid = db.insert_pipeline_schedule(1, '* * * * *', '', 0, 0, 0, 0)

        assert rowid == 1

    def test_insert_pipeline_schedule_not_empty_cron_or_rundate(self, db) -> None:

        db.create_schema()

        with pytest.raises(dbError):
            db.insert_pipeline_schedule(1, '* * * * *', '2022-12-20 00:00:00', 0, 0, 0, 0)
        db.close()

    def test_insert_pipeline_schedule_empty_cron(self, db, pipeline) -> None:

        db.insert_pipeline_schedule(1, '', '2022-12-20 00:00:00', 0, 0, 0, 0)

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '', '2022-12-20 00:00:00', 0, 0, 0, 0)]
        db.close()

    def test_insert_pipeline_schedule_empty_at(self, db, pipeline) -> None:

        db.insert_pipeline_schedule(1, '* * * * *', '', 0, 0, 0, 0)

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '* * * * *', '', 0, 0, 0, 0)]
        db.close()

    def test_insert_pipeline_schedule_both_empty_cron_and_at(self, db, pipeline) -> None:

        db.insert_pipeline_schedule(1, '', '', 0, 0, 0, 0)

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '', '', 0, 0, 0, 0)]
        db.close()

    def test_insert_pipeline_schedule_no_pipeline(self, db) -> None:

        db.create_schema()

        with pytest.raises(dbError):
            db.insert_pipeline_schedule(1, '', '2022-12-20 00:00:00', 0, 0, 0, 0)
        db.close()

    def test_update_pipeline_schedule_cron(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_cron(1, '*/5 * * * *')

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '*/5 * * * *', '', 0, 0, 0, 0)]
        db.close()

    def test_update_pipeline_schedule_cron_existing_at(self, db, pipeline, schedule_at) -> None:

        with pytest.raises(dbError):
            db.update_pipeline_schedule_cron(1, '*/5 * * * *')
        db.close()

    def test_update_pipeline_schedule_cron_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_cron(2, '*/5 * * * *')

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '* * * * *', '', 0, 0, 0, 0)]
        db.close()

    def test_update_pipeline_schedule_at(self, db, pipeline, schedule_at) -> None:

        db.update_pipeline_schedule_at(1, '2023-01-01 00:00:00')

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)]
        db.close()

    def test_update_pipeline_schedule_at_existing_cron(self, db, pipeline, schedule_cron) -> None:

        with pytest.raises(dbError):
            db.update_pipeline_schedule_at(1, '2023-01-01 00:00:00')
        db.close()

    def test_update_pipeline_schedule_at_wrong_id(self, db, pipeline, schedule_at) -> None:

        db.update_pipeline_schedule_at(2, '2025-01-01 00:00:00')

        query = """
                SELECT pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries
                FROM pipeline_schedule
                WHERE id = 1
                """
        results = db._conn.cursor().execute(query)

        assert results.fetchall() == [(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)]
        db.close()

    def test_update_pipeline_schedule_paused(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_paused(1, 1)

        query = "SELECT paused FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 1

    def test_update_pipeline_schedule_paused_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_paused(2, 1)

        query = "SELECT paused FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 0

    def test_update_pipeline_schedule_retry_on_crash(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_retry_on_crash(1, 1)

        query = "SELECT retry_on_crash FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 1

    def test_update_pipeline_schedule_retry_on_crash_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_retry_on_crash(2, 1)

        query = "SELECT retry_on_crash FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 0

    def test_update_pipeline_schedule_retry_num(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_retry_num(1, 1)

        query = "SELECT retry_num FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 1

    def test_update_pipeline_schedule_retry_num_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_retry_num(2, 1)

        query = "SELECT retry_num FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 0

    def test_update_pipeline_schedule_max_retries(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_max_retries(1, 1)

        query = "SELECT max_retries FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 1

    def test_update_pipeline_schedule_max_retries_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_max_retries(2, 1)

        query = "SELECT max_retries FROM pipeline_schedule WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 0

    def test_update_pipeline_schedule_latest_run(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_latest_run(1, 5)
        query = "SELECT latest_run from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 5

    def test_update_pipeline_schedule_latest_run_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_latest_run(2, 5)
        query = "SELECT latest_run from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    # ##### PIPELINE RUN TABLE TESTS ##### #

    def test_insert_pipeline_run_return_id(self, db, pipeline) -> None:

        db.create_schema()
        rowid = db.insert_pipeline_schedule(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)

        assert rowid == 1

    def test_insert_pipeline_run_no_pipeline(self, db) -> None:

        db.create_schema()

        with pytest.raises(dbError):
            db.insert_pipeline_run(1, 'running', '2023-01-01 00:00:00')
        db.close()

    def test_insert_pipeline_run_empty_data(self, db, pipeline) -> None:

        with pytest.raises(dbError):
            db.insert_pipeline_run(1, '', '')
        db.close()

    def test_insert_pipeline_run(self, db, pipeline, schedule_cron) -> None:

        db.insert_pipeline_run(1, 1, 'running', '2023-01-01 00:00:00')

        query = "SELECT pipeline_id, schedule_id, status, start_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == (1, 1, 'running', '2023-01-01 00:00:00')
        db.close()

    def test_update_pipeline_run_status(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_status(1, 'crashed')

        query = "SELECT status FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('crashed',)
        db.close()

    def test_update_pipeline_run_status_wrong_id(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_status(2, 'crashed')

        query = "SELECT status FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('running',)
        db.close()

    def test_update_pipeline_run_stage(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_stage(1, 4)

        query = "SELECT stage FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == (4,)
        db.close()

    def test_update_pipeline_run_stage_wrong_id(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_stage(2, 4)

        query = "SELECT stage FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None
        db.close()

    def test_update_pipeline_run_stage_msg(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_stage_msg(1, 'this is a stage msg')

        query = "SELECT stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('this is a stage msg',)
        db.close()

    def test_update_pipeline_run_stage_msg_wrong_id(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_stage_msg(2, 'this is a stage msg')

        query = "SELECT stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None
        db.close()

    def test_update_pipeline_run_exit_msg(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_exit_msg(1, 'this is a exit msg')

        query = "SELECT exit_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('this is a exit msg',)
        db.close()

    def test_update_pipeline_run_exit_msg_wrong_id(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_exit_msg(2, 'this is a exit msg')

        query = "SELECT exit_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None
        db.close()

    def test_update_pipeline_run_end_time(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_end_time(1, '2025-01-01 00:00:00')

        query = "SELECT end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('2025-01-01 00:00:00',)
        db.close()

    def test_update_pipeline_run_end_time_wrong_id(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_end_time(2, '2025-01-01 00:00:00')

        query = "SELECT end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None
        db.close()

    # ##### COMPOUND WRITE TESTS ##### #

    def test_update_pipeline_run_stage_and_stage_msg(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_stage_and_stage_msg(1, 4, 'this is a stage msg')

        query = "SELECT stage, stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == (4, 'this is a stage msg')
        db.close()

    def test_update_pipeline_run_status_exit_msg_end_time(self, db, pipeline, schedule_cron, run) -> None:

        db.update_pipeline_run_status_exit_msg_end_time(1, 'crashed', 'stacktrace', '2025-03-03 00:00:00')

        query = "SELECT status, exit_msg, end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone() == ('crashed', 'stacktrace', '2025-03-03 00:00:00')
        db.close()

    # ##### CLI COMMAND TESTS ##### #

    def test_summary_pipelines(self, db, pipeline, schedule_cron, run) -> None:

        results = db.summary_pipelines()

        assert results == [('test', 'test.py', 1, '* * * * *', '', 0, None, None, None)]
        db.close()

    # ##### DB READS TESTS ##### #

    def test_all_pipelines(self, db, pipeline) -> None:

        results = db.all_pipelines()

        assert results == [(1, 'test', 'test.py', 'test_dir', 111.1)]
        db.close()

    def test_all_pipelines_no_pipelines(self, db) -> None:

        db.create_schema()
        results = db.all_pipelines()

        assert results == []
        db.close()

    def test_all_pipelines_scheduling(self, db, pipeline, schedule_cron) -> None:

        results = db.all_pipelines_scheduling()

        assert results == [(1, 'test', 'test.py', 'test_dir', 1, '* * * * *', '', 0)]
        db.close()

    def test_all_pipelines_scheduling_no_pipline(self, db) -> None:

        db.create_schema()
        results = db.all_pipelines_scheduling()

        assert results == []
        db.close()

    def test_pipeline_schedule(self, db, pipeline, schedule_cron) -> None:

        results = db.pipeline_schedule(1, 1)

        assert results == (1, 'test', 'test.py', 'test_dir', 1, '* * * * *', '', 0, None)
        db.close()

    def test_pipeline_schedule_no_pipeline(self, db, pipeline) -> None:

        results = db.pipeline_schedule(1, 1)

        assert results is None
        db.close()

    def test_pipeline_from_schedule_id(self, db, pipeline, schedule_at) -> None:

        results = db.pipeline_from_schedule_id(1)

        assert results == (1, 'test', 'test.py', 'test_dir')
        db.close()

    def test_pipeline_from_schedule_id_no_id(self, db, pipeline, schedule_at) -> None:

        results = db.pipeline_from_schedule_id(10)

        assert results is None
        db.close()

    def test_pipeline(self, db, pipeline) -> None:

        results = db.pipeline(1)

        assert results == (1, 'test', 'test.py', 'test_dir', 111.1)
        db.close()

    def test_pipeline_no_pipeline(self, db) -> None:

        db.create_schema()
        results = db.pipeline(1)

        assert results is None
        db.close()

    def test_pipeline_id_from_name(self, db, pipeline_name) -> None:

        results = db.pipeline_id_from_name('test-name')

        assert results == 1
        db.close()

    def test_pipeline_id_from_name_no_pipline(self, db) -> None:

        db.create_schema()
        results = db.pipeline_id_from_name('test')

        assert results is None
        db.close()

    def test_pipeline_id_from_tuple(self, db, pipeline) -> None:

        results = db.pipeline_id_from_tuple('test.py', 'test_dir')

        assert results == 1
        db.close()

    def test_pipeline_id_from_tuple_no_pipeline(self, db) -> None:

        db.create_schema()
        results = db.pipeline_id_from_tuple('test.py', 'test_dir')

        assert results is None
        db.close()

    def test_pipeline_py_from_name(self, db, pipeline) -> None:

        results = db.pipeline_py_from_name('test')

        assert results == 'test.py'
        db.close()

    def test_pipeline_py_from_name_no_pipeline(self, db) -> None:

        db.create_schema()
        results = db.pipeline_py_from_name('test')

        assert results is None
        db.close()

    def test_pipeline_dir_from_name(self, db, pipeline) -> None:

        results = db.pipeline_dir_from_name('test')

        assert results == 'test_dir'
        db.close()

    def test_pipeline_dir_from_name_no_pipeline(self, db) -> None:

        db.create_schema()
        results = db.pipeline_dir_from_name('test')

        assert results is None
        db.close()

    def test_pipeline_schedule_at(self, db, pipeline, schedule_at) -> None:

        results = db.pipeline_schedule_at(1)

        assert results == '2023-01-01 00:00:00'
        db.close()

    def test_pipeline_schedule_at_no_schedule(self, db, pipeline) -> None:

        results = db.pipeline_schedule_at(1)

        assert results is None
        db.close()

    def test_pipeline_schedule_cron(self, db, pipeline, schedule_cron) -> None:

        results = db.pipeline_schedule_cron(1)

        assert results == '* * * * *'
        db.close()

    def test_pipeline_schedule_cron_no_schedule(self, db, pipeline) -> None:

        results = db.pipeline_schedule_cron(1)

        assert results is None
        db.close()

    def test_pipeline_schedules_id(self, db, pipeline, schedule_cron) -> None:

        results = db.pipeline_schedules_id(1)

        assert results == [1]
        db.close()

    def test_pipeline_schedules_id_no_schedule(self, db, pipeline) -> None:

        results = db.pipeline_schedules_id(1)

        assert results == []
        db.close()

    def test_pipeline_run_id_by_pipeline_id_and_start_time(self, db, pipeline, schedule_cron, run) -> None:

        results = db.pipeline_run_id_by_pipeline_id_and_start_time(1, '2023-01-01 00:00:00')

        assert results == 1
        db.close()

    def test_pipeline_run_id_by_pipeline_id_and_start_time_no_run(self, db, pipeline) -> None:

        results = db.pipeline_run_id_by_pipeline_id_and_start_time(1, '2023-01-01 00:00:00')

        assert results is None
        db.close()
