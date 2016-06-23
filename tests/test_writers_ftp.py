import mock
import tempfile
import unittest
from contextlib import closing

from ozzy.records.base_record import BaseRecord
from ozzy.writers import FTPWriter
from ozzy.writers.base_writer import InconsistentWriteState

from .utils import meta


class FTPWriterTest(unittest.TestCase):

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    def test_default_port_is_21(self):
        options = dict(options=dict(
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test/'))
        with closing(FTPWriter(options, meta())) as writer:
            self.assertEquals(21, writer.read_option('port'))

    @mock.patch('ozzy.writers.FTPWriter.build_ftp_instance')
    def test_create_parent_dirs_in_right_order(self, mock_ftp):
        filebase = 'some/long/dir/tree/'
        options = dict(options=dict(
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase=filebase))
        with tempfile.NamedTemporaryFile() as tmp:
            with closing(FTPWriter(options, meta())) as writer:
                writer.write(tmp.name)

        mock_mkd = mock_ftp.return_value.mkd
        self.assertEquals([
            mock.call('some'),
            mock.call('some/long'),
            mock.call('some/long/dir'),
            mock.call('some/long/dir/tree'),
        ], mock_mkd.mock_calls)

    @mock.patch('ozzy.writers.FTPWriter.build_ftp_instance')
    def test_create_parent_dirs_with_filebase_prefix(self, mock_ftp):
        filebase = 'some/long/dir/with/prefix'
        options = dict(options=dict(
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase=filebase))
        with tempfile.NamedTemporaryFile() as tmp:
            with closing(FTPWriter(options, meta())) as writer:
                writer.write(tmp.name)

        mock_mkd = mock_ftp.return_value.mkd
        self.assertEquals([
            mock.call('some'),
            mock.call('some/long'),
            mock.call('some/long/dir'),
            mock.call('some/long/dir/with'),
        ], mock_mkd.mock_calls)

    @mock.patch('ozzy.writers.FTPWriter.build_ftp_instance')
    def test_check_writer_consistency_unexpected_size(self, mock_ftp):

        # given
        options = dict(options=dict(
            check_consistency=True,
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test/',))
        mock_ftp.return_value.size.return_value = 999

        # when:
        with closing(FTPWriter(options, meta())) as writer:
            writer.write_batch(self.get_batch())
            writer.flush()
            # then
            with self.assertRaisesRegexp(InconsistentWriteState, 'Unexpected size for file'):
                writer.finish_writing()

    @mock.patch('ozzy.writers.FTPWriter.build_ftp_instance')
    def test_check_writer_consistency_not_present(self, mock_ftp):
        # given
        options = dict(options=dict(
            check_consistency=True,
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test/',))
        mock_ftp.return_value.size.return_value = -1

        # when:
        with closing(FTPWriter(options, meta())) as writer:
            writer.write_batch(self.get_batch())
            writer.flush()
            # then
            with self.assertRaisesRegexp(InconsistentWriteState,
                                         'file is not present at destination'):
                writer.finish_writing()
