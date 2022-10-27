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
        # TODO: should all this be in the same table???
        self._conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS pipeline(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                py_name TEXT,
                dir_name TEXT,
                cron TEXT,
                paused TEXT,
                status TEXT,
                stage INTEGER,
                msg TEXT,
                stacktrace TEXT
            )""")
        self._conn.commit()

    def pipeline_details(self, name: str) -> list:

        query = "SELECT name, py_name, dir_name, cron FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()

    def ls_pipelines_details(self) -> list:

        results = self._conn.cursor().execute("""
            SELECT id, name, py_name, dir_name, cron, paused, status, stage, msg FROM pipeline
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

    def pipeline_py_name(self, name: str) -> str:

        query = "SELECT py_name FROM pipeline WHERE name = ?"
        params = (name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_dir_name(self, pipeline_name: str) -> str:

        query = "SELECT dir_name FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_cron(self, pipeline_name: str) -> str:

        query = "SELECT cron FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._conn.cursor().execute(query, params)
        return results.fetchone()[0]

    def pipeline_run_details(self) -> list:

        query = "SELECT id, name, py_name, dir_name, cron FROM pipeline"
        results = self._conn.cursor().execute(query)
        return results.fetchall()

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

    def pipeline_set_stage(self, id: int, stage: int, msg: str) -> None:

        query = "UPDATE pipeline SET stage = ?, msg = ? WHERE id = ?"
        params = (stage, msg, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_set_status(self, id: int, status: str) -> None:

        query = "UPDATE pipeline SET status = ? WHERE id = ?"
        params = (status, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_set_stacktrace(self, id: int, stacktrace: str) -> None:

        query = "UPDATE pipeline SET stacktrace = ? WHERE id = ?"
        params = (stacktrace, id)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_insert(self, name: str, py_name: str, dir_name: str, cron: str) -> None:

        query = "INSERT INTO pipeline VALUES (NULL, ?, ?, ?, ?, FALSE, NULL, NULL, NULL, NULL)"
        params = (name, py_name, dir_name, cron)
        self._conn.cursor().execute(query, params)
        self._conn.commit()

    def pipeline_delete(self, id: int) -> None:

        query = "DELETE FROM pipeline WHERE rowid = ?"
        params = (id,)
        self._conn.cursor().execute(query, params)
        self._conn.commit()
