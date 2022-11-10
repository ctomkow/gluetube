# Craig Tomkow
# 2022-11-08

# local imports

from gluetube.autodiscovery import PipelineScanner
# for some reason, from gluetube.exception import AutodiscoveryError doesn't work, but this does
#   and if works ONLY if the import is after the PipelineScanner import (where sys.path if modified in __init__.py)
from exception import AutodiscoveryError

# 3rd part imports
import pytest
import os
from pathlib import Path


class TestPipelineScanner:

    @pytest.fixture
    def scanner(self):

        return PipelineScanner(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'pipeline_dir'))

    @pytest.fixture
    def abspath_test_dir(self) -> Path:

        return Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'pipeline_dir'))

    def test_scanner_no_pipeline_dir(self) -> None:

        with pytest.raises(AutodiscoveryError):
            PipelineScanner(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'no_exists_dir'))

    def test_all_dirs(self, scanner, abspath_test_dir) -> None:

        dirs = scanner._all_dirs(scanner.pipeline_dir)

        test_1_dir = Path(f'{abspath_test_dir}/test_1')
        test_2_dir = Path(f'{abspath_test_dir}/test_2')

        assert dirs == [test_1_dir, test_2_dir]
