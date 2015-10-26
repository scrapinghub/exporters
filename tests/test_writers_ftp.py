import unittest
from exporters.writers import FTPWriter


class FTPWriterTest(unittest.TestCase):

    def test_default_port_is_21(self):
        options = dict(options=dict(
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test/'))
        writer = FTPWriter(options)
        self.assertEquals(21, writer.read_option('port'))
        writer.close_writer()

    def test_not_path_filebase(self):
        options = dict(options=dict(
            ftp_user='user',
            ftp_password='password',
            host='ftp.example.com',
            filebase='test'))
        writer = FTPWriter(options)
        self.assertIs(writer._create_target_dir_if_needed(''), None)
        writer.close_writer()
