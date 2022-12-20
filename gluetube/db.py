# Craig Tomkow
# 2022-10-14

# local imports
import exception
import util

# python imports
import sqlite3
from pathlib import Path
from typing import Union, List, Tuple


class Database:

    _conn = None

    def __init__(self, db_path: Path = Path('.'), read_only: bool = True, in_memory: bool = False) -> None:

        if in_memory:
            self._conn = sqlite3.connect("file::memory:")
            self._conn.execute('pragma journal_mode=wal;')
            self._conn.execute('pragma foreign_keys=ON;')
        else:
            if read_only:
                self._conn = sqlite3.connect(f"{db_path.absolute().as_uri()}?mode=ro", uri=True)
            else:
                self._conn = sqlite3.connect(db_path.absolute().as_posix())
                self._conn.execute('pragma journal_mode=wal;')
                self._conn.execute('pragma foreign_keys=ON;')

    def close(self) -> None:

        self._conn.close()


class Store(Database):

    token = None

    def __init__(self, token: str, db_path: Path = Path('.'), read_only: bool = True, in_memory: bool = False) -> None:

        self.token = token

        super().__init__(db_path, read_only, in_memory)

    def create_table(self, table: str) -> None:

        self._conn.cursor().execute(f"""
            CREATE TABLE IF NOT EXISTS {table}(
                key TEXT UNIQUE NOT NULL CHECK (key != ''),
                value TEXT NOT NULL CHECK (value != '')
            )""")
        self._conn.commit()

    def all_key_values(self, table: str) -> list:

        try:
            query = f"SELECT * FROM {table}"
            results = self._conn.cursor().execute(query)
            return results.fetchall()
        except sqlite3.OperationalError as e:
            raise exception.dbError(f"Failed database query. {e}") from e

    def all_keys(self, table: str) -> list:

        try:
            query = f"SELECT key FROM {table}"
            results = self._conn.cursor().execute(query)
            return results.fetchall()
        except sqlite3.OperationalError as e:
            raise exception.dbError(f"Failed database query. {e}") from e

    def value(self, table: str, key: str) -> str:

        query = f"SELECT value FROM {table} WHERE key = ?"
        params = (key,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return util.decrypt(data[0], self.token)
        else:
            return data

    def insert_key_value(self, table: str, key: str, value: str) -> None:

        try:
            query = f"INSERT OR REPLACE INTO {table} VALUES (?, ?)"
            params = (key, util.encrypt(value, self.token))
            self._conn.cursor().execute(query, params)
            self._conn.commit()
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

    def delete_key(self, table: str, key: str) -> None:

        query = f"DELETE FROM {table} WHERE key = ?"
        params = (key,)
        self._conn.cursor().execute(query, params)
        self._conn.commit()


class Pipeline(Database):

    def create_schema(self) -> None:

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline(
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT UNIQUE NOT NULL CHECK (name != ''),
                py_name TEXT NOT NULL CHECK (py_name != ''),
                dir_name TEXT NOT NULL CHECK (dir_name != ''),
                py_timestamp REAL NOT NULL CHECK (py_timestamp != '')
            )""")
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline_schedule(
                id INTEGER PRIMARY KEY NOT NULL,
                pipeline_id INTEGER NOT NULL,
                cron TEXT,
                at TEXT,
                paused INTEGER,
                retry_on_crash INTEGER,
                retry_num INTEGER,
                max_retries INTEGER,
                latest_run INTEGER,
                CHECK(
                    ((cron IS NULL OR cron = '') AND (at IS NULL OR at = ''))
                    OR
                    ((cron IS NOT NULL OR cron != '') AND (at IS NULL OR at = ''))
                    OR
                    ((cron IS NULL OR cron = '') AND (at IS NOT NULL OR at != ''))
                ),
                CONSTRAINT fk_piplineschedule_pipeline
                    FOREIGN KEY(pipeline_id)
                    REFERENCES pipeline(id)
                    ON DELETE CASCADE
            )""")
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline_run(
                id INTEGER PRIMARY KEY NOT NULL,
                pipeline_id INTEGER NOT NULL,
                schedule_id INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status != ''),
                stage INTEGER,
                stage_msg TEXT,
                exit_msg TEXT,
                start_time TEXT NOT NULL CHECK (start_time != ''),
                end_time TEXT,
                CONSTRAINT fk_piplinerun_pipeline
                    FOREIGN KEY(pipeline_id)
                    REFERENCES pipeline(id)
                    ON DELETE CASCADE,
                CONSTRAINT fk_pipelinerun_pipeline_schedule
                    FOREIGN KEY(schedule_id)
                    REFERENCES pipeline_schedule(id)
                    ON DELETE CASCADE
            )""")
        self._conn.commit()

        # pre-optimization, i know i know
        self._conn.cursor().execute("""
            CREATE INDEX IF NOT EXISTS pipeline_id_index ON pipeline_run (pipeline_id)
            """)
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE INDEX IF NOT EXISTS stage_index ON pipeline_run (stage)
            """)
        self._conn.commit()

        self._conn.cursor().execute("""
            CREATE INDEX IF NOT EXISTS start_time_index ON pipeline_run (start_time)
            """)
        self._conn.commit()

    # pipeline writes

    def insert_pipeline(self, name: str, py_name: str, dir_name: str, py_timestamp: str) -> int:

        try:
            query = "INSERT INTO pipeline VALUES (NULL, ?, ?, ?, ?)"
            params = (name, py_name, dir_name, py_timestamp)
            rowid = self._conn.cursor().execute(query, params).lastrowid
            self._conn.commit()
            return rowid
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

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

    # pipeline_schedule writes

    def delete_pipeline_schedule(self, schedule_id: int) -> None:

        query = "DELETE FROM pipeline_schedule WHERE id = ?"
        params = (schedule_id,)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def insert_pipeline_schedule(self, pipeline_id: int, cron: str = '', at: str = '', paused: int = 0,
                                 retry_on_crash: int = 0, retry_num: int = 0, max_retries: int = 0) -> int:

        try:
            query = "INSERT INTO pipeline_schedule VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, NULL)"
            params = (pipeline_id, cron, at, paused, retry_on_crash, retry_num, max_retries)
            rowid = self._conn.cursor().execute(query, params).lastrowid
            self._conn.commit()
            return rowid
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

    def update_pipeline_schedule_cron(self, schedule_id: int, cron: str) -> None:

        try:
            query = "UPDATE pipeline_schedule SET cron = ? WHERE id = ?"
            params = (cron, schedule_id)
            self._conn.cursor().execute(query, params)
            self._conn.commit()
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

    def update_pipeline_schedule_at(self, schedule_id: int, at: str) -> None:

        try:
            query = "UPDATE pipeline_schedule SET at = ? WHERE id = ?"
            params = (at, schedule_id)
            self._conn.cursor().execute(query, params)
            self._conn.commit()
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

    def update_pipeline_schedule_paused(self, schedule_id: int, paused: int) -> None:

        query = "UPDATE pipeline_schedule SET paused = ? WHERE id = ?"
        params = (paused, schedule_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_retry_on_crash(self, schedule_id: int, retry_on_crash: int) -> None:

        query = "UPDATE pipeline_schedule SET retry_on_crash = ? WHERE id = ?"
        params = (retry_on_crash, schedule_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_retry_num(self, schedule_id: int, retry_num: int) -> None:

        query = "UPDATE pipeline_schedule SET retry_num = ? WHERE id = ?"
        params = (retry_num, schedule_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_max_retries(self, schedule_id: int, max_retries: int) -> None:

        query = "UPDATE pipeline_schedule SET max_retries = ? WHERE id = ?"
        params = (max_retries, schedule_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def update_pipeline_schedule_latest_run(self, schedule_id: int, run_id: int) -> None:

        query = "UPDATE pipeline_schedule SET latest_run = ? WHERE id = ?"
        params = (run_id, schedule_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    # pipeline_run writes

    def insert_pipeline_run(self, pipeline_id: int, schedule_id: int, status: str = '', start_time: str = '') -> int:

        try:
            query = "INSERT INTO pipeline_run VALUES (NULL, ?, ?, ?, NULL, NULL, NULL, ?, NULL)"
            params = (pipeline_id, schedule_id, status, start_time)
            rowid = self._conn.cursor().execute(query, params).lastrowid
            self._conn.commit()
            return rowid
        except sqlite3.IntegrityError as e:
            raise exception.dbError(f"Failed database insert. {e}") from e

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

    def summary_pipelines(self) -> List[Tuple[str, str, int, str, str, int, str, str, str]]:

        results = self._conn.cursor().execute("""
            SELECT pipeline.name, pipeline.py_name, pipeline_schedule.id, pipeline_schedule.cron, pipeline_schedule.at,
                   pipeline_schedule.paused, pipeline_run.status, pipeline_run.stage_msg, pipeline_run.end_time
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id
            LEFT JOIN pipeline_run
            ON pipeline_schedule.latest_run = pipeline_run.id
        """)
        return results.fetchall()

    # reads

    def all_pipelines(self) -> List[Tuple[int, str, str, str, float, int]]:

        query = "SELECT id, name, py_name, dir_name, py_timestamp FROM pipeline"
        results = self._conn.cursor().execute(query)
        return results.fetchall()

    def all_pipelines_scheduling(self) -> List[Tuple[int, str, str, str, int, str, str, int]]:

        results = self._conn.cursor().execute("""
            SELECT pipeline.id, pipeline.name, pipeline.py_name, pipeline.dir_name,
                   pipeline_schedule.id, pipeline_schedule.cron,
                   pipeline_schedule.at, pipeline_schedule.paused
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id;
        """)
        return results.fetchall()

    def pipeline_schedule(self, pipeline_id: int, schedule_id: int) -> List[Tuple[int, str, str, str, int, str, str, int, int]]:

        query = """
            SELECT pipeline.id, pipeline.name, pipeline.py_name, pipeline.dir_name,
                   pipeline_schedule.id, pipeline_schedule.cron,
                   pipeline_schedule.at, pipeline_schedule.paused, pipeline_schedule.latest_run
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id
            WHERE pipeline.id = ? AND pipeline_schedule.id = ?;
        """
        params = (pipeline_id, schedule_id)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchall()
        if data:
            return data[0]
        else:
            return None

    def pipeline_from_schedule_id(self, schedule_id: int) -> Union[Tuple[int, str, str, str], None]:

        query = """
            SELECT pipeline.id, pipeline.name, pipeline.py_name, pipeline.dir_name
            FROM pipeline
            LEFT JOIN pipeline_schedule
            ON pipeline.id = pipeline_schedule.pipeline_id
            WHERE pipeline_schedule.id = ?;
        """
        params = (schedule_id,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchall()
        if data:
            return data[0]
        else:
            return None

    def pipeline(self, pipeline_id: int) -> Union[tuple, None]:

        query = """
            SELECT id, name, py_name, dir_name, py_timestamp
            FROM pipeline
            WHERE id = ?
            """
        params = (pipeline_id,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()

    def pipeline_id_from_name(self, name: str) -> Union[int, None]:

        query = "SELECT id FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_id_from_tuple(self, py_name: str, dir_name: str) -> Union[int, None]:

        query = "SELECT id FROM pipeline WHERE py_name = ? AND dir_name = ?"
        params = (py_name, dir_name)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_py_from_name(self, name: str) -> Union[str, None]:

        query = "SELECT py_name FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_dir_from_name(self, pipeline_name: str) -> Union[str, None]:

        query = "SELECT dir_name FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_schedule_at(self, schedule_id: int) -> Union[str, None]:

        query = "SELECT at FROM pipeline_schedule WHERE id = ?"
        params = (schedule_id,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_schedule_cron(self, schedule_id: int) -> Union[str, None]:

        query = "SELECT cron FROM pipeline_schedule WHERE id = ?"
        params = (schedule_id,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_schedules_id(self, pipeline_id: int) -> List[int]:

        query = "SELECT id FROM pipeline_schedule WHERE pipeline_id = ?"
        params = (pipeline_id,)
        results = self._conn.cursor().execute(query, params)

        return [x[0] for x in results.fetchall()]

    def pipeline_run_id_by_pipeline_id_and_start_time(self, pipeline_id: int, start_time: str) -> Union[int, None]:

        query = "SELECT id FROM pipeline_run WHERE pipeline_id = ? AND start_time = ?"
        params = (pipeline_id, start_time)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchone()
        if data:
            return data[0]
        else:
            return data

    def pipeline_run(self, run_id: int) -> Union[Tuple[int, int, str, int, str, str, str, str], None]:

        query = """
            SELECT pipeline_id, schedule_id, status, stage, stage_msg, exit_msg, start_time, end_time
            FROM pipeline_run
            WHERE pipeline_run.id = ?;
        """
        params = (run_id,)
        results = self._conn.cursor().execute(query, params)
        data = results.fetchall()
        if data:
            return data[0]
        else:
            return None
