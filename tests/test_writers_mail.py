import unittest

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers import MailWriter



class FakeMailWriter(MailWriter):

    def __init__(self, options, *args, **kwargs):
        self.send_called_number = 0
        super(FakeMailWriter, self).__init__(options, *args, **kwargs)

    def _write_mail(self, dump_path, group_key):
        self.send_called_number += 1


class MailWriterTest(unittest.TestCase):

    def setUp(self):
        self.writer_config = {
            'options': {
                'emails': [],
                'subject': 'test',
                'from': 'test',
                'access_key': 'test',
                'secret_key': 'test'
            }
        }
        self.batch_path = 'some_path'

    def test_write_no_items(self):
        writer = FakeMailWriter(self.writer_config, export_formatter=JsonExportFormatter(dict()))
        writer.write(self.batch_path, [])
        self.assertEqual(writer.send_called_number, 0)
        writer.writer_metadata['items_count'] = 1
        writer.write(self.batch_path, [])
        self.assertEqual(writer.send_called_number, 1)
        writer.close()
