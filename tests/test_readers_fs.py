from gzip import GzipFile

from exporters.readers import FSReader
from exporters.exceptions import ConfigurationError

from .utils import meta

import pytest


class FSReaderTest(object):
    @classmethod
    def setup_class(cls):
        cls.options = {
            'input': {
                'dir': './tests/data/fs_reader_test',
            }
        }

        cls.options_pointer = {
            'input': {
                'dir_pointer': './tests/data/fs_reader_pointer',
            }
        }

        cls.options_empty_folder = {
            'input': {
                'dir': './tests/data/fs_reader_empty_folder',
            }
        }

    @staticmethod
    def _make_fs_reader(options):
        full_config = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': options
        }
        reader = FSReader(full_config, meta())
        reader.set_last_position(None)
        return reader

    def test_read_from_folder(self):
        expected = [
            {u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'},
            {u'item2': u'value1'}, {u'item2': u'value2'}, {u'item2': u'value3'},
        ]
        reader = self._make_fs_reader(self.options)
        batch = list(reader.get_next_batch())
        assert expected == batch

    def test_read_from_pointer(self):
        expected = [
            {u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'},
            {u'item2': u'value1'}, {u'item2': u'value2'}, {u'item2': u'value3'},
        ]
        reader = self._make_fs_reader(self.options_pointer)
        batch = list(reader.get_next_batch())
        assert expected == batch

    def test_read_from_empty_folder(self):
        reader = self._make_fs_reader(self.options_empty_folder)
        list(reader.get_next_batch())
        assert reader.is_finished()

    def test_read_from_file(self):
        reader = self._make_fs_reader({
            'input': './tests/data/fs_reader_test/fs_test_data.jl.gz',
        })
        batch = list(reader.get_next_batch())
        expected = [
            {u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}
        ]
        assert expected == batch

    def test_read_from_multiple_files(self):
        reader = self._make_fs_reader({
            'input': [
                './tests/data/fs_reader_test/fs_test_data.jl.gz',
                './tests/data/fs_reader_test/fs_test_data_2.jl.gz',
            ]
        })
        batch = list(reader.get_next_batch())
        expected = [
            {u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'},
            {u'item2': u'value1'}, {u'item2': u'value2'}, {u'item2': u'value3'},
        ]
        assert expected == batch

    def test_read_from_file_and_dir(self):
        reader = self._make_fs_reader({
            'input': [
                './tests/data/fs_reader_test/fs_test_data.jl.gz',
                {'dir': './tests/data/fs_reader_test'}
            ]
        })
        batch = list(reader.get_next_batch())
        expected = [
            {u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'},
            {u'item2': u'value1'}, {u'item2': u'value2'}, {u'item2': u'value3'},
        ]
        assert expected == batch

    def test_dir_specification_no_dir_or_dir_pointer(self):
        with pytest.raises(ConfigurationError) as err:
            self._make_fs_reader({'input': {}})
        assert str(err.value) == ('Input directory dict must contain "dir"'
                                  ' or "dir_pointer" element (but not both)')

    def test_dir_specification_both_dir_and_dir_pointer(self):
        with pytest.raises(ConfigurationError) as err:
            self._make_fs_reader({
                'input': {'dir': './foo', 'dir_pointer': './bar'}
            })
        assert str(err.value) == ('Input directory dict must not contain both'
                                  ' "dir" and "dir_pointer" elements')

    def test_dir_specification_with_pattern(self):
        reader = self._make_fs_reader({
            'input': {
                'dir': './tests/data/fs_reader_test/',
                'pattern': 'fs_reader_test/[^/]+2\.jl\.gz$'
            }
        })
        expected = [
            {u'item2': u'value1'}, {u'item2': u'value2'}, {u'item2': u'value3'},
        ]
        batch = list(reader.get_next_batch())
        assert expected == batch

    def test_dot_files_ignored_by_default(self, tmpdir_with_dotfiles):
        reader = self._make_fs_reader({'input': {
            'dir': tmpdir_with_dotfiles.strpath,
        }})
        assert list(reader.get_next_batch()) == [{"bar": 1}]

        reader = self._make_fs_reader({'input': {
            'dir': tmpdir_with_dotfiles.strpath,
            'pattern': r'/\.[^/]*$',
        }})
        assert list(reader.get_next_batch()) == []

    def test_dot_files_included_with_flag(self, tmpdir_with_dotfiles):
        reader = self._make_fs_reader({'input': {
            'dir': tmpdir_with_dotfiles.strpath,
            'pattern': r'/\.[^/]*$',
            'include_dot_files': True,
        }})
        assert list(reader.get_next_batch()) == [{"foo": 1}]

        reader = self._make_fs_reader({'input': {
            'dir': tmpdir_with_dotfiles.strpath,
            'include_dot_files': True,
        }})
        assert list(reader.get_next_batch()) == [{"foo": 1}, {"bar": 1}]


@pytest.fixture
def tmpdir_with_dotfiles(tmpdir):
    with GzipFile(tmpdir.join('.foo.jl.gz').strpath, 'w') as zf:
        zf.write('{"foo": 1}')
    with GzipFile(tmpdir.join('bar.jl.gz').strpath, 'w') as zf:
        zf.write('{"bar": 1}')
    return tmpdir
