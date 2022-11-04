# Craig Tomkow
# 2022-10-14

# local imports
import config
import util
import exceptions

# python imports
import sqlite3


class Database:

    _conn = None

    def __init__(self, db_name: str, read_only: bool = True) -> None:

        try:
            gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        except (exceptions.ConfigFileParseError, exceptions.ConfigFileNotFoundError) as e:
            raise exceptions.dbError(f"Failed to initialize database. {e}") from e
        if read_only:
            self._conn = sqlite3.connect(f"file:{gt_cfg.database_dir}/{db_name}?mode=ro", uri=True)
        else:
            self._conn = sqlite3.connect(f"{gt_cfg.database_dir}/{db_name}")
            self._conn.execute('pragma journal_mode=wal;')
            self._conn.execute('pragma foreign_keys=ON;')

    def close(self) -> None:

        self._conn.close()


class Store(Database):

    def create_table(self, table: str) -> None:

        self._conn.cursor().execute(f"""
            CREATE TABLE IF NOT EXISTS {table}(
                key TEXT UNIQUE,
                value TEXT
            )""")
        self._conn.commit()

    def all_key_values(self, table: str) -> list:

        query = f"SELECT * FROM {table}"
        results = self._conn.cursor().execute(query)
        return results.fetchall()

    def insert_key_value(self, table: str, key: str, value: str) -> None:

        query = f"INSERT OR REPLACE INTO {table} VALUES (?, ?)"
        params = (key, value)
        self._conn.cursor().execute(query, params)
        self._conn.commit()


class Pipeline(Database):

    def create_schema(self) -> None:

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                py_name TEXT,
                dir_name TEXT,
                py_timestamp TEXT,
                latest_run INTEGER
            )""")
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline_schedule(
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER,
                cron TEXT,
                run_date TEXT,
                paused INTEGER,
                retry_on_crash INTEGER,
                retry_num INTEGER,
                max_retries INTEGER,
                CHECK(
                    ((cron IS NULL OR cron = '') AND (run_date IS NULL OR run_date = ''))
                    OR
                    ((cron IS NOT NULL OR cron != '') AND (run_date IS NULL OR run_date = ''))
                    OR
                    ((cron IS NULL OR cron = '') AND (run_date IS NOT NULL OR run_date != ''))
                ),
                CONSTRAINT fk_piplineschedule_pipeline
                    FOREIGN KEY(pipeline_id)
                    REFERENCES pipeline(id)
                    ON DELETE CASCADE
            )""")
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline_run(
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER,
                status TEXT,
                stage INTEGER,
                stage_msg TEXT,
                exit_msg TEXT,
                start_time TEXT,
                end_time TEXT,
                CONSTRAINT fk_piplinerun_pipeline
                    FOREIGN KEY(pipeline_id)
                    REFERENCES pipeline(id)
                    ON DELETE CASCADE
            )""")
        self._conn.commit()

        # pre-optimization, i know i know
        self._conn.cursor().execute("""
            CREATE INDEX pipeline_id_index ON pipeline_run (pipeline_id)
            """)
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE INDEX stage_index ON pipeline_run (stage)
            """)
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE INDEX start_time_index ON pipeline_run (start_time)
            """)
        self._conn.commit()

    # pipeline writes

    def insert_pipeline(self, name: str, py_name: str, dir_name: str, py_timestamp: str) -> None:

        query = "INSERT INTO pipeline VALUES (NULL, ?, ?, ?, ?, NULL)"
        params = (name, py_name, dir_name, py_timestamp)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def delete_pipeline(self, pipeline_id: int) -> None:

        query = "DELETE FROM pipeline WHERE id = ?"
        params = (pipeline_id,)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_name(self, pipeline_id: int, name: str) -> None:

        query = "UPDATE pipeline SET name = ? WHERE id = ?"
        params = (name, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_py_timestamp(self, pipeline_id: int, timestamp: str) -> None:

        query = "UPDATE pipeline SET py_timestamp = ? WHERE id = ?"
        params = (timestamp, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_latest_run(self, pipeline_id: int, run_id: int) -> None:

        query = "UPDATE pipeline SET latest_run = ? WHERE id = ?"
        params = (run_id, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    # pipeline_schedule writes

    def insert_pipeline_schedule(self, pipeline_id: int, cron: str = '', run_date: str = '', paused: int = 0,
                                 retry_on_crash: int = 0, retry_num: int = 0, max_retries: int = 0) -> None:

        query = "INSERT INTO pipeline_schedule VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)"
        params = (pipeline_id, cron, run_date, paused, retry_on_crash, retry_num, max_retries)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_cron(self, pipeline_id: int, cron: str) -> None:

        query = "UPDATE pipeline_schedule SET cron = ? WHERE pipeline_id = ?"
        params = (cron, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_run_date(self, pipeline_id: int, run_date: str) -> None:

        query = "UPDATE pipeline_schedule SET run_date = ? WHERE pipeline_id = ?"
        params = (run_date, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_paused(self, pipeline_id: int, paused: int) -> None:

        query = "UPDATE pipeline_schedule SET paused = ? WHERE pipeline_id = ?"
        params = (paused, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_retry_on_crash(self, pipeline_id: int, retry_on_crash: int) -> None:

        query = "UPDATE pipeline_schedule SET retry_on_crash = ? WHERE pipeline_id = ?"
        params = (retry_on_crash, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_retry_num(self, pipeline_id: int, retry_num: int) -> None:

        query = "UPDATE pipeline_schedule SET retry_num = ? WHERE pipeline_id = ?"
        params = (retry_num, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_max_retries(self, pipeline_id: int, max_retries: int) -> None:

        query = "UPDATE pipeline_schedule SET max_retries = ? WHERE pipeline_id = ?"
        params = (max_retries, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    # pipeline_run writes

    def insert_pipeline_run(self, pipeline_id: int, status: str = '', start_time: str = '') -> None:

        query = "INSERT INTO pipeline_run VALUES (NULL, ?, ?, NULL, NULL, NULL, ?, NULL)"
        params = (pipeline_id, status, start_time)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_status(self, pipeline_run_id: int, status: str) -> None:

        query = "UPDATE pipeline_run SET status = ? WHERE id = ?"
        params = (status, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_stage(self, pipeline_run_id: int, stage: int) -> None:

        query = "UPDATE pipeline_run SET stage = ? WHERE id = ?"
        params = (stage, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_stage_msg(self, pipeline_run_id: int, msg: str) -> None:

        query = "UPDATE pipeline_run SET stage_msg = ? WHERE id = ?"
        params = (msg, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_exit_msg(self, pipeline_run_id: int, msg: str) -> None:

        query = "UPDATE pipeline_run SET exit_msg = ? WHERE id = ?"
        params = (msg, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_end_time(self, pipeline_run_id: int, end_time: str) -> None:

        query = "UPDATE pipeline_run SET end_time = ? WHERE id = ?"
        params = (end_time, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    # compound writes

    def update_pipeline_run_stage_and_stage_msg(self, pipeline_run_id: int, stage: int, msg: str) -> None:

        query = "UPDATE pipeline_run SET stage = ?, stage_msg = ? WHERE id = ?"
        params = (stage, msg, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_run_status_exit_msg_end_time(self, pipeline_run_id: int, status: str, msg: str, end_time: str) -> None:

        query = "UPDATE pipeline_run SET status = ?, exit_msg = ?, end_time = ? WHERE id = ?"
        params = (status, msg, end_time, pipeline_run_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    # cli commands

    def ls_pipelines(self) -> list:

        results = self._conn.cursor().execute("""
            SELECT pipeline.name, pipeline_schedule.cron, pipeline_schedule.run_date, pipeline_schedule.paused,
                   pipeline_run.status, pipeline_run.stage_msg, pipeline_run.end_time
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id
            LEFT JOIN pipeline_run
            ON pipeline.id = pipeline_run.pipeline_id AND pipeline.latest_run = pipeline_run.id;
        """)
        return results.fetchall()

    # reads

    def all_pipelines(self) -> list:

        query = "SELECT id, name, py_name, dir_name, py_timestamp, latest_run FROM pipeline"
        results = self._conn.cursor().execute(query)
        return results.fetchall()

    def all_pipelines_scheduling(self) -> list:

        results = self._conn.cursor().execute("""
            SELECT pipeline.id, pipeline.name, pipeline.py_name, pipeline.dir_name, pipeline_schedule.cron,
                   pipeline_schedule.run_date, pipeline_schedule.paused
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id;
        """)
        return results.fetchall()

    def pipeline_id_from_name(self, name: str) -> int:

        query = "SELECT id FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_id_from_tuple(self, py_name: str, dir_name: str) -> int:

        query = "SELECT id FROM pipeline WHERE py_name = ? AND dir_name = ?"
        params = (py_name, dir_name)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_py_from_name(self, name: str) -> str:

        query = "SELECT py_name FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_dir_from_name(self, pipeline_name: str) -> str:

        query = "SELECT dir_name FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_run_id_by_pipeline_id_and_start_time(self, pipeline_id: int, start_time: str) -> int:

        query = "SELECT id FROM pipeline_run WHERE pipeline_id = ? AND start_time = ?"
        params = (pipeline_id, start_time)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]
