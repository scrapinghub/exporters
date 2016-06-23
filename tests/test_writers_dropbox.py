import mock
import unittest
from ozzy.records.base_record import BaseRecord
from ozzy.writers.dropbox_writer import DropboxWriter

from .utils import meta


class DropboxWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'ozzy.writers.dropbox_writer.DropboxWriter',
            'options': {
                "access_token": "some_token",
                "filebase": "/test/sh_file_"
            }
        }

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    @mock.patch('dropbox.Dropbox.files_upload_session_finish')
    @mock.patch('dropbox.Dropbox.files_upload_session_append')
    @mock.patch('dropbox.Dropbox.files_upload_session_start')
    def test_write_batch(self, start_mock, append_mock, finish_mock):

        # given
        start_mock.return_value.session_id = '123'
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        writer = DropboxWriter(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.get_metadata('items_count'), 2)
