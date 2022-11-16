# Craig Tomkow
# 2022-09-13

# local imports
from gluetube import util

# python imports
import json
import struct


def test_append_name_to_dir_list() -> None:

    name = 'test_str'
    dirs = ['dir_one/', 'dir_two/']

    result = util.append_name_to_dir_list(name, dirs)
    assert result == [
            'dir_one/test_str',
            'dir_two/test_str'
        ]


def test_conf_dir() -> None:

    result = util.conf_dir()

    assert all(isinstance(elem, str) for elem in result)


def test_craft_rpc_msg() -> None:

    msg_bytes = str.encode(json.dumps({'func': 'myfunc', 'params': ['a', 'b']}))
    test_payload = struct.pack('>I', len(msg_bytes)) + msg_bytes

    result = util.craft_rpc_msg('myfunc', ['a', 'b'])

    assert result == test_payload
