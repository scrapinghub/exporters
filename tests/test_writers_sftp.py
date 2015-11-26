import unittest

from exporters.writers import SFTPWriter


class SFTPWriterTest(unittest.TestCase):

    def test_create(self):
        options = {
            'sftp_user': 'user',
            'sftp_password': 'password',
            'filebase': 'test/',
            'host': 'sftp.example.com',
        }
        writer = SFTPWriter(dict(options=options))
        self.assertEquals(22, writer.read_option('port'))
        writer.close()
