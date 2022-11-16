# 2022-11-15
# Craig Tomkow

# local imports
from gluetube.config import Gluetube
from exception import ConfigFileParseError, ConfigFileNotFoundError

# python imports
from pathlib import Path

# 3rd party imports
import pytest


class TestGluetube:

    def test_config(self) -> None:

        gt_cfg = Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'gluetube.cfg').resolve().as_posix())

        assert isinstance(gt_cfg, Gluetube)

    def test_config_cfg_file_doesnt_exist(self) -> None:

        with pytest.raises(ConfigFileNotFoundError):
            Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'dummy.cfg').resolve().as_posix())

    def test_config_cfg_file_malformed(self) -> None:

        with pytest.raises(ConfigFileParseError):
            Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'malformed.cfg').resolve().as_posix())

    def test_config_parse(self) -> None:

        gt_cfg = Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'gluetube.cfg').resolve().as_posix())
        gt_cfg.parse()

        assert gt_cfg.pid_file == '/tmp/gluetube.pid'

    def test_config_parse_invalid_key(self) -> None:

        with pytest.raises(ConfigFileParseError):
            gt_cfg = Gluetube(Path(Path(__file__).parent.resolve(), 'cfg', 'bad_key.cfg').resolve().as_posix())
            gt_cfg.parse()
