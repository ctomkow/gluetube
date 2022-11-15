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

# TODO: continue unit testing the database
