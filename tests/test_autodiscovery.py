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
        test_dirs = [Path(f'{abspath_test_dir}/test_2'),
                     Path(f'{abspath_test_dir}/test_1')]

        assert set(dirs) == set(test_dirs)

    def test_all_py_files(self, scanner, abspath_test_dir) -> None:

        py_files = scanner._all_py_files(Path(f"{abspath_test_dir}/test_1"))
        test_files = [Path(f"{abspath_test_dir}/test_1/example_pipeline1.py"),
                      Path(f"{abspath_test_dir}/test_1/example_pipeline2.py")]

        assert set(py_files) == set(test_files)

    def test_enumerate_fs_pipelines(self, scanner, abspath_test_dir) -> None:

        tuples = scanner._enumerate_fs_pipelines([Path(f"{abspath_test_dir}/test_1")])
        test_tuples = [('example_pipeline2.py', 'test_1', 1667941329.6938233),
                       ('example_pipeline1.py', 'test_1', 1667941307.4055572)]

        assert set(tuples) == set(test_tuples)

    def test_enumerate_db_pipelines(self, scanner) -> None:

        enumerated = scanner._enumerate_db_pipelines([(1, 'myname', 'file.py', 'mydir', 1.1, 1)])
        test_tuples = [('file.py', 'mydir', 1.1)]

        assert set(enumerated) == set(test_tuples)

    def test_cmp_two_elems_a_diff_b(self, scanner) -> None:

        list_a = ['a', 'b', 'd']
        list_b = ['c', 'b', 'a']
        result = scanner._cmp_two_elems(list_a, list_b, 'a_diff_b')

        assert result == ['d']

    def test_cmp_two_elems_b_diff_a(self, scanner) -> None:

        list_a = ['a', 'b', 'd']
        list_b = ['c', 'b', 'a']
        result = scanner._cmp_two_elems(list_a, list_b, 'b_diff_a')

        assert result == ['c']

    def test_cmp_two_elems_diff_both(self, scanner) -> None:

        list_a = ['a', 'b', 'd']
        list_b = ['c', 'b', 'a']
        result = scanner._cmp_two_elems(list_a, list_b, 'diff_both')

        assert set(result) == set(['c', 'd'])

    def test_cmp_two_elems_same_both(self, scanner) -> None:

        list_a = ['a', 'b', 'd']
        list_b = ['c', 'b', 'a']
        result = scanner._cmp_two_elems(list_a, list_b, 'same_both')

        assert set(result) == set(['a', 'b'])
