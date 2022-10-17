# Craig Tomkow
# 2022-09-13

# local imports
from gluetube import command

# 3rd part imports
import pytest


@pytest.fixture
def conf_dir():

    return [
            './',
            'cfg/',
            '~/.config/gluetube/',
            '/usr/local/etc/gluetube/',
            '/etc/opt/gluetube/',
            '/etc/gluetube/'
        ]


@pytest.fixture
def conf_name():

    return 'test.cfg'


def test_append_conf_name_to_dir(conf_name, conf_dir):

    result = command._append_conf_name_to_dir(conf_name, conf_dir)
    assert result == [
            './test.cfg',
            'cfg/test.cfg',
            '~/.config/gluetube/test.cfg',
            '/usr/local/etc/gluetube/test.cfg',
            '/etc/opt/gluetube/test.cfg',
            '/etc/gluetube/test.cfg'
        ]
