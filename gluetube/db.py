# Craig Tomkow
# 2022-10-14

# local imports
import config
import util

# python imports
import sqlite3


class Database:

    def __init__(self, db_name: str) -> None:

        gt_cfg = config.Gluetube(util.append_name_to_dir_list('gluetube.cfg', util.conf_dir()))
        self.con = sqlite3.connect(f"{gt_cfg.database_dir}/{db_name}")
        self._cur = self.con.cursor()


class Store(Database):

    def create_table(self, table: str) -> None:

        self._cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}(
                key TEXT UNIQUE,
                value TEXT
            )""")

    def all_key_values(self, table: str) -> list:

        query = f"SELECT * FROM {table}"
        results = self._cur.execute(query)
        return results.fetchall()

    def insert_key_value(self, table: str, key: str, value: str) -> None:

        query = f"INSERT OR REPLACE INTO {table} VALUES (?, ?)"
        params = (key, value)
        self._cur.execute(query, params)
        self.con.commit()


class Pipeline(Database):

    def create_schema(self) -> None:

        self._cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                py_name TEXT,
                dir_name TEXT,
                cron TEXT
            )""")

    def all_pipelines(self) -> list:

        results = self._cur.execute("""
            SELECT name FROM pipeline
        """)
        return results.fetchall()

    def pipeline_py_name(self, pipeline_name: str) -> str:

        query = "SELECT py_name FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._cur.execute(query, params)
        return results.fetchone()

    def pipeline_dir_name(self, pipeline_name: str) -> str:

        query = "SELECT dir_name FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._cur.execute(query, params)
        return results.fetchone()

    def pipeline_cron(self, pipeline_name: str) -> str:

        query = "SELECT cron FROM pipeline WHERE name = ?"
        params = (pipeline_name,)
        results = self._cur.execute(query, params)
        return results.fetchone()

    def pipeline_run_details(self) -> list:

        query = "SELECT name, py_name, dir_name, cron FROM pipeline"
        results = self._cur.execute(query)
        return results.fetchall()
