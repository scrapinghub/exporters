import unittest
from exporters.writers import MailWriter

from exporters.writers.odo_writer import ODOWriter


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
        self.batch_path = 'not_a_path'

    def test_write_no_items(self):
        writer = MailWriter(self.writer_config)
        writer.write(self.batch_path, [])
        writer.items_count = 1
        with self.assertRaises(OSError):
            writer.write(self.batch_path, [])
