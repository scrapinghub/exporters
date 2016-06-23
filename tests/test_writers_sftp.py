import unittest

import errno
import mock
from ozzy.records.base_record import BaseRecord
from ozzy.writers import SFTPWriter
from ozzy.writers.base_writer import InconsistentWriteState

from .utils import meta


class SFTPWriterTest(unittest.TestCase):

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    def get_writer_config(self):
        return {
            'name': 'ozzy.writers.sftp_writer.SFTPWriter',
            'options': {
                'sftp_user': 'user',
                'sftp_password': 'password',
                'filebase': 'test/',
                'host': 'sftp.example.com',
            }
        }

    def test_create(self):
        options = self.get_writer_config()
        writer = SFTPWriter(options, meta())
        self.assertEquals(22, writer.read_option('port'))
        writer.close()

    @mock.patch('pysftp.Connection')
    def test_check_writer_consistency(self, mock_sftp):

        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True
        mock_sftp.return_value.__enter__.return_value.stat.return_value.st_size = 999

        # when:
        try:
            writer = SFTPWriter(options, meta())
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaisesRegexp(InconsistentWriteState, 'Wrong size for file'):
            writer.finish_writing()

        exception = IOError()
        exception.errno = errno.ENOENT
        mock_sftp.return_value.__enter__.return_value.stat.side_effect = exception
        with self.assertRaisesRegexp(InconsistentWriteState, 'is not present at destination'):
            writer.finish_writing()
