import unittest

from exporters.writers import MailWriter

from .utils import meta
import copy


class FakeMailWriter(MailWriter):

    def __init__(self, *args, **kwargs):
        self.send_called_number = 0
        super(FakeMailWriter, self).__init__(*args, **kwargs)

    def _write_mail(self, dump_path, group_key):
        self.send_called_number += 1


WRITER_CONFIG = {
    'emails': [],
    'subject': 'test',
    'from': 'test',
    'access_key': 'test',
    'secret_key': 'test'
}


class MailWriterTest(unittest.TestCase):

    def get_writer_config(self, **kwargs):
        config = copy.deepcopy(WRITER_CONFIG)
        config.update(**kwargs)
        return {'options': config}

    def setUp(self):
        self.batch_path = 'some_path'

    def test_write_no_items(self):
        writer_config = self.get_writer_config()
        writer = FakeMailWriter(writer_config, meta())
        writer.write(self.batch_path, [])
        self.assertEqual(writer.send_called_number, 0)
        writer.set_metadata('items_count', 1)
        writer.write(self.batch_path, [])
        self.assertEqual(writer.send_called_number, 1)
        writer.close()

    def test_file_name_none_compression(self):
        writer_config = self.get_writer_config(file_name='some_file_', compression='none')
        print writer_config
        writer = FakeMailWriter(
            writer_config, meta())
        writer.set_metadata('items_count', 1)
        writer.write(self.batch_path, [])
        self.assertEqual('some_file_0.jl', writer._get_file_name())
        writer.close()

    def test_file_name_default_compression(self):
        writer_config = self.get_writer_config(file_name='some_file_')
        print writer_config
        writer = FakeMailWriter(
            writer_config, meta())
        writer.set_metadata('items_count', 1)
        writer.write(self.batch_path, [])
        self.assertEqual('some_file_0.jl.gz', writer._get_file_name())
        writer.close()

    def test_file_name_bz2_compression(self):
        writer_config = self.get_writer_config(file_name='some_file_', compression='bz2')
        print writer_config
        writer = FakeMailWriter(
            writer_config, meta())
        writer.set_metadata('items_count', 1)
        writer.write(self.batch_path, [])
        self.assertEqual('some_file_0.jl.bz2', writer._get_file_name())
        writer.close()
