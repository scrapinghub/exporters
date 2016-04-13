import mock
import unittest

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.records.base_record import BaseRecord
from exporters.writers.gdrive_writer import GDriveWriter
from exporters.writers.base_writer import InconsistentWriteState

from .utils import meta


class GStorageWriterTest(unittest.TestCase):
    def get_items_to_write(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    def get_options(self):
        return {
            'name': 'exporters.writers.gdrive_writer.GDriveWriter',
            'options': {
                "filebase": "test",
                "client_secret": {},
                "credentials": {},
            }
        }

    @mock.patch('pydrive.auth.GoogleAuth')
    @mock.patch('pydrive.drive.GoogleDrive')
    def test_write(self, drive, auth):
        items_to_write = self.get_items_to_write()
        options = self.get_options()

        writer = GDriveWriter(
            options, meta(), export_formatter=JsonExportFormatter(dict()))
        writer.write_batch(items_to_write)
        writer.flush()
        writer.close()
        drive.assert_has_calls([('call().CreateFile().Upload()', (mock.ANY,))])

    @mock.patch('pydrive.auth.GoogleAuth')
    @mock.patch('pydrive.drive.GoogleDrive')
    def test_write_blob_consistency(self, drive, auth):
        drive().CreateFile()['size'] = 999
        drive().CreateFile()['md5Checksum'] = "a"*24
        # given
        items_to_write = self.get_items_to_write()
        options = self.get_options()
        options['options']['check_consistency'] = True

        # when:
        writer = GDriveWriter(
            options, meta(), export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaises(InconsistentWriteState):
            writer.finish_writing()
