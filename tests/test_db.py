# Craig Tomkow
# 2022-11-14

# local imports
from gluetube.db import Store, Pipeline
from exception import dbError

# 3rd party imports
import pytest


class TestStore:

    @pytest.fixture
    def db(self) -> Store:

        return Store(in_memory=True)

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

        assert results.fetchone()[0] == 'pass_asdf'
        db.close()

    def test_insert_key_empty_value(self, db) -> None:

        db.create_table('TABLEA')
        with pytest.raises(dbError):
            db.insert_key_value('TABLEA', 'user_bob', '')
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

        assert results == [('user_bob', 'pass_asdf')]
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
    def schedule_cron(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_schedule(1, '* * * * *', '', 0, 0, 0, 0)

    @pytest.fixture
    def schedule_rundate(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_schedule(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)

    @pytest.fixture
    def run(self, db) -> None:

        db.create_schema()
        db.insert_pipeline_run(1, 'running', '2023-01-01 00:00:00')

    def test_create_schema(self, db) -> None:

        db.create_schema()
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([('pipeline',), ('pipeline_schedule',), ('pipeline_run',)])
        db.close()

    def test_create_schema_tables_exist_with_data(self, db, pipeline) -> None:

        db.create_schema()
        query = "SELECT name, py_name, dir_name, py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['test', 'test.py', 'test_dir', 111.1])
        db.close()

    # ##### PIPELINE TABLE TESTS ##### #

    def test_insert_pipeline(self, db, pipeline) -> None:

        query = "SELECT name, py_name, dir_name, py_timestamp from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['test', 'test.py', 'test_dir', 111.1])
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

    def test_update_pipeline_latest_run(self, db, pipeline) -> None:

        db.update_pipeline_latest_run(1, 5)
        query = "SELECT latest_run from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] == 5

    def test_update_pipeline_latest_run_wrong_id(self, db, pipeline) -> None:

        db.update_pipeline_latest_run(2, 5)
        query = "SELECT latest_run from pipeline where id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    # ##### PIPELINE SCHEDULE TABLE TESTS ##### #

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

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '', '2022-12-20 00:00:00', 0, 0, 0, 0)])
        db.close()

    def test_insert_pipeline_schedule_empty_rundate(self, db, pipeline) -> None:

        db.insert_pipeline_schedule(1, '* * * * *', '', 0, 0, 0, 0)

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '* * * * *', '', 0, 0, 0, 0)])
        db.close()

    def test_insert_pipeline_schedule_both_empty_cron_and_rundate(self, db, pipeline) -> None:

        db.insert_pipeline_schedule(1, '', '', 0, 0, 0, 0)

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '', '', 0, 0, 0, 0)])
        db.close()

    def test_insert_pipeline_schedule_no_pipeline(self, db) -> None:

        db.create_schema()

        with pytest.raises(dbError):
            db.insert_pipeline_schedule(1, '', '2022-12-20 00:00:00', 0, 0, 0, 0)
        db.close()

    def test_update_pipeline_schedule_cron(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_cron(1, '*/5 * * * *')

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '*/5 * * * *', '', 0, 0, 0, 0)])
        db.close()

    def test_update_pipeline_schedule_cron_existing_rundate(self, db, pipeline, schedule_rundate) -> None:

        with pytest.raises(dbError):
            db.update_pipeline_schedule_cron(1, '*/5 * * * *')
        db.close()

    def test_update_pipeline_schedule_cron_wrong_id(self, db, pipeline, schedule_cron) -> None:

        db.update_pipeline_schedule_cron(2, '*/5 * * * *')

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '* * * * *', '', 0, 0, 0, 0)])
        db.close()

    def test_update_pipeline_schedule_run_date(self, db, pipeline, schedule_rundate) -> None:

        db.update_pipeline_schedule_run_date(1, '2023-01-01 00:00:00')

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)])
        db.close()

    def test_update_pipeline_schedule_run_date_existing_cron(self, db, pipeline, schedule_cron) -> None:

        with pytest.raises(dbError):
            db.update_pipeline_schedule_run_date(1, '2023-01-01 00:00:00')
        db.close()

    def test_update_pipeline_schedule_run_date_wrong_id(self, db, pipeline, schedule_rundate) -> None:

        db.update_pipeline_schedule_run_date(2, '2025-01-01 00:00:00')

        query = "SELECT pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries from pipeline_schedule where id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchall()) == set([(1, '', '2023-01-01 00:00:00', 0, 0, 0, 0)])
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

    def test_insert_pipeline_run(self, db, pipeline) -> None:

        db.insert_pipeline_run(1, 'running', '2023-01-01 00:00:00')

        query = "SELECT pipeline_id, status, start_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set([1, 'running', '2023-01-01 00:00:00'])

    def test_update_pipeline_run_status(self, db, pipeline, run) -> None:

        db.update_pipeline_run_status(1, 'crashed')

        query = "SELECT status FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['crashed'])

    def test_update_pipeline_run_status_wrong_id(self, db, pipeline, run) -> None:

        db.update_pipeline_run_status(2, 'crashed')

        query = "SELECT status FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['running'])

    def test_update_pipeline_run_stage(self, db, pipeline, run) -> None:

        db.update_pipeline_run_stage(1, 4)

        query = "SELECT stage FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set([4])

    def test_update_pipeline_run_stage_wrong_id(self, db, pipeline, run) -> None:

        db.update_pipeline_run_stage(2, 4)

        query = "SELECT stage FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    def test_update_pipeline_run_stage_msg(self, db, pipeline, run) -> None:

        db.update_pipeline_run_stage_msg(1, 'this is a stage msg')

        query = "SELECT stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['this is a stage msg'])

    def test_update_pipeline_run_stage_msg_wrong_id(self, db, pipeline, run) -> None:

        db.update_pipeline_run_stage_msg(2, 'this is a stage msg')

        query = "SELECT stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    def test_update_pipeline_run_exit_msg(self, db, pipeline, run) -> None:

        db.update_pipeline_run_exit_msg(1, 'this is a exit msg')

        query = "SELECT exit_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['this is a exit msg'])

    def test_update_pipeline_run_exit_msg_wrong_id(self, db, pipeline, run) -> None:

        db.update_pipeline_run_exit_msg(2, 'this is a exit msg')

        query = "SELECT exit_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    def test_update_pipeline_run_end_time(self, db, pipeline, run) -> None:

        db.update_pipeline_run_end_time(1, '2025-01-01 00:00:00')

        query = "SELECT end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['2025-01-01 00:00:00'])

    def test_update_pipeline_run_end_time_wrong_id(self, db, pipeline, run) -> None:

        db.update_pipeline_run_end_time(2, '2025-01-01 00:00:00')

        query = "SELECT end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert results.fetchone()[0] is None

    # ##### COMPOUND WRITE TESTS ##### #

    def test_update_pipeline_run_stage_and_stage_msg(self, db, pipeline, run) -> None:

        db.update_pipeline_run_stage_and_stage_msg(1, 4, 'this is a stage msg')

        query = "SELECT stage, stage_msg FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set([4, 'this is a stage msg'])

    def test_update_pipeline_run_status_exit_msg_end_time(self, db, pipeline, run) -> None:

        db.update_pipeline_run_status_exit_msg_end_time(1, 'crashed', 'stacktrace', '2025-03-03 00:00:00')

        query = "SELECT status, exit_msg, end_time FROM pipeline_run WHERE id = 1"
        results = db._conn.cursor().execute(query)

        assert set(results.fetchone()) == set(['crashed', 'stacktrace', '2025-03-03 00:00:00'])
