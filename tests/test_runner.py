# 2022-11-17
# Craig Tomkow

# local imports
from gluetube import runner
from gluetube.db import Store

# python imports
from pathlib import Path

# 3rd party imports
from jinja2 import Environment, FileSystemLoader, Template, exceptions
import pytest


def test_load_template_env() -> None:

    env = runner._load_template_env(Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'test_1'))
    assert env.list_templates() == ['a_fake_pipeline.sh', 'example_pipeline1.py', 'example_pipeline2.py']


def test_load_template_env_no_dir() -> None:

    env = runner._load_template_env(Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'no_exists_dir'))
    assert env.list_templates() == []


def test_jinja_template() -> None:

    file_loader = FileSystemLoader(Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'test_1').resolve().as_posix())
    env = Environment(loader=file_loader)

    template = runner._jinja_template(env, 'example_pipeline1.py')
    assert isinstance(template, Template)


def test_jinja_template_no_file() -> None:

    file_loader = FileSystemLoader(Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'test_1').resolve().as_posix())
    env = Environment(loader=file_loader)

    with pytest.raises(exceptions.TemplateNotFound):
        runner._jinja_template(env, 'no_exists_pipeline.py')


def test_all_variables_in_template() -> None:

    directory = Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'test_1')
    file_loader = FileSystemLoader(directory.as_posix())
    env = Environment(loader=file_loader)

    variables = runner._all_variables_in_template(env, directory, 'example_pipeline1.py')

    assert variables == {'API1_PASSWORD', 'API1_USERNAME'}


def test_all_variables_in_template_no_variable() -> None:

    directory = Path(Path(__file__).parent.resolve(), 'pipeline_dir', 'test_1')
    file_loader = FileSystemLoader(directory.as_posix())
    env = Environment(loader=file_loader)

    variables = runner._all_variables_in_template(env, directory, 'example_pipeline1.py')

    assert variables != {'no_variable'}


def test_variable_value_pairs_for_template() -> None:

    db = Store('PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=', in_memory=True)
    db.create_table('common')
    db.insert_key_value('common', 'USERNAME', 'alice')

    pairs = runner._variable_value_pairs_for_template(('USERNAME',), db)

    assert pairs == {'USERNAME': "'alice'"}
    db.close()


def test_variable_value_pairs_for_template_bad_key() -> None:

    db = Store('PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=', in_memory=True)
    db.create_table('common')
    query = "INSERT OR REPLACE INTO common VALUES (?, ?)"
    params = ('USERNAME', 'alice')
    db._conn.cursor().execute(query, params)
    db._conn.commit()

    pairs = runner._variable_value_pairs_for_template(('BAD_KEY',), db)

    assert pairs == {}
    db.close()
