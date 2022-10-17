# Craig Tomkow
# 2022-10-14

# python imports
import sqlite3


class Database:

    def __init__(self, db_name: str) -> None:

        # TODO: change static path to dynamic location
        self.con = sqlite3.connect(f"/home/gluetube/.gluetube/db/{db_name}")
        self._cur = self.con.cursor()

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
