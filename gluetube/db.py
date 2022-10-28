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

        # TODO: add a timestamp to table, to confirm start time and finish time
        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                py_name TEXT,
                dir_name TEXT,
                cron TEXT,
                paused TEXT,
                current_run INTEGER
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
                FOREIGN KEY(pipeline_id) REFERENCES pipeline(id)
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

    def pipeline_insert(self, name: str, py_name: str, dir_name: str, cron: str) -> None:

        query = "INSERT INTO pipeline VALUES (NULL, ?, ?, ?, ?, FALSE, NULL)"
        params = (name, py_name, dir_name, cron)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_delete(self, id: int) -> None:

        query = "DELETE FROM pipeline WHERE rowid = ?"
        params = (id,)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def all_pipelines(self) -> list:

        query = "SELECT id, name, py_name, dir_name, cron, paused, current_run FROM pipeline"
        results = self._conn.cursor().execute(query)
        return results.fetchall()

    def ls_pipelines(self) -> list:
        # TODO: finish this
        results = self._conn.cursor().execute("""
            SELECT pipeline.name, pipeline.cron, pipeline.paused, pipeline_run.status, pipeline_run.stage_msg, pipeline_run.end_time
            FROM pipeline
            LEFT JOIN pipeline_run
            ON pipeline.id = pipeline_run.pipeline_id AND pipeline.current_run = pipeline_run.id;
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

    def pipeline_cron_from_name(self, pipeline_name: str) -> str:

        query = "SELECT cron FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_set_cron(self, id: int, cron: str) -> None:

        query = "UPDATE pipeline SET cron = ? WHERE id = ?"
        params = (cron, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_set_py(self, name: str, py_name: str) -> None:

        query = "UPDATE pipeline SET py_name = ? WHERE name = ?"
        params = (py_name, name)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_set_current_run(self, pipeline_id: int, pipeline_run_id: int) -> None:

        query = "UPDATE pipeline SET current_run = ? WHERE id = ?"
        params = (pipeline_run_id, pipeline_id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_run_insert(self, pipeline_id: int, status: str, start_time: str) -> None:

        query = "INSERT INTO pipeline_run VALUES (NULL, ?, ?, NULL, NULL, NULL, ?, NULL)"
        params = (pipeline_id, status, start_time)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_run_id_by_pipeline_id_and_start_time(self, pipeline_id: int, start_time: str) -> int:

        query = "SELECT id FROM pipeline_run WHERE pipeline_id = ? AND start_time = ?"
        params = (pipeline_id, start_time)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_run_set_stage_and_msg(self, id: int, stage: int, msg: str) -> None:

        query = "UPDATE pipeline_run SET stage = ?, stage_msg = ? WHERE id = ?"
        params = (stage, msg, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_run_set_status(self, id: int, status: str) -> None:

        query = "UPDATE pipeline_run SET status = ? WHERE id = ?"
        params = (status, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_run_set_finished(self, id: int, status: str, msg: str, end_time: str) -> None:

        query = "UPDATE pipeline_run SET status = ?, exit_msg = ?, end_time = ? WHERE id = ?"
        params = (status, msg, end_time, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()
