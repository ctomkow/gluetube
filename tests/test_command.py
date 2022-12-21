# Craig Tomkow
# 2022-12-21

# local imports
from gluetube.db import Store
from gluetube.command import db_rekey
from gluetube.config import Gluetube

# python imports
from pathlib import Path

# 3rd party imports
from cryptography.fernet import Fernet


def test_db_rekey() -> None:

    db = Store('PjhSLgp2FbZqbdMzwLEPK-VRaIBiiN_WwEwnAnqhA_o=', in_memory=True)
    db.create_table('common')
    db.insert_key_value('common', 'USERNAME', 'asdf')
    db.insert_key_value('common', 'PASSWORD', 'secret')
    new_key = Fernet.generate_key()
    gt_cfg = Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'gluetube.cfg').resolve().as_posix())