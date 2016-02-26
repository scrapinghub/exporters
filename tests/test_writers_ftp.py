import mock
import tempfile
import unittest
from contextlib import closing

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers import FTPWriter


class FTPWriterTest(unittest.TestCase):

    def test_default_port_is_21(self):
        options = dict(options=dict(
            generate_md5=False,
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test/'))
        with closing(FTPWriter(options, export_formatter=JsonExportFormatter(dict()))) as writer:
            self.assertEquals(21, writer.read_option('port'))

    @mock.patch('exporters.writers.FTPWriter.build_ftp_instance')
    def test_create_parent_dirs_in_right_order(self, mock_ftp):
        filebase = 'some/long/dir/tree/'
        options = dict(options=dict(
            generate_md5=False,
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase=filebase))
        with tempfile.NamedTemporaryFile() as tmp:
            with closing(FTPWriter(options, export_formatter=JsonExportFormatter(dict()))) as writer:
                writer.write(tmp.name)

        mock_mkd = mock_ftp.return_value.mkd
        self.assertEquals([
            mock.call('some'),
            mock.call('some/long'),
            mock.call('some/long/dir'),
            mock.call('some/long/dir/tree'),
        ], mock_mkd.mock_calls)

    @mock.patch('exporters.writers.FTPWriter.build_ftp_instance')
    def test_create_parent_dirs_with_filebase_prefix(self, mock_ftp):
        filebase = 'some/long/dir/with/prefix'
        options = dict(options=dict(
            generate_md5=False,
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase=filebase))
        with tempfile.NamedTemporaryFile() as tmp:
            with closing(FTPWriter(options, export_formatter=JsonExportFormatter(dict()))) as writer:
                writer.write(tmp.name)

        mock_mkd = mock_ftp.return_value.mkd
        self.assertEquals([
            mock.call('some'),
            mock.call('some/long'),
            mock.call('some/long/dir'),
            mock.call('some/long/dir/with'),
        ], mock_mkd.mock_calls)
