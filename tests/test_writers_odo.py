import os
import shutil
import tempfile
import unittest

from exporters.writers.odo_writer import ODOWriter


class OdoWriterTest(unittest.TestCase):

    def setUp(self):
        self.batch_path = 'tests/data/test_data.jl.gz'

        self.tmp_path = tempfile.mkdtemp()
        self.tmp_file = os.path.join(self.tmp_path, 'test.csv')

        self.schema = {'$schema': u'http://json-schema.org/draft-04/schema',
                       'required': [u'item'], 'type': 'object',
                       'properties': {u'item': {'type': 'string'}}}
        self.writer_config = {
            'options': {
                'odo_uri': self.tmp_file,
                'schema': self.schema
            }
        }

    @unittest.skipIf(os.getenv('DRONE_BUILD'), 'feature disabled for quick turnaround in deploy')
    def test_write_csv(self):
        writer = ODOWriter(self.writer_config)
        writer.write(self.batch_path, [])
        writer.close()
        with open(self.tmp_file) as f:
            lines = f.readlines()
        self.assertEqual(lines, ['item\n', 'value1\n', 'value2\n', 'value3\n'])
        shutil.rmtree(self.tmp_path)
