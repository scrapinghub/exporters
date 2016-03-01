import unittest

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers import SFTPWriter


class SFTPWriterTest(unittest.TestCase):

    def test_create(self):
        options = {
            'sftp_user': 'user',
            'sftp_password': 'password',
            'filebase': 'test/',
            'host': 'sftp.example.com',
            'check_consistency': False
        }
        writer = SFTPWriter(dict(options=options), export_formatter=JsonExportFormatter(dict()))
        self.assertEquals(22, writer.read_option('port'))
        writer.close()
