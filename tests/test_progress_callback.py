from __future__ import absolute_import
import unittest
from exporters.progress_callback import (
    FtpDownloadProgress,
    FtpUploadProgress,
    BotoUploadProgress,
)


class FakeLogger(object):
    def __init__(self):
        self.messages = []

    def info(self, mesg):
        self.messages.append(mesg)


class ProgressCallbackTest(unittest.TestCase):
    def setUp(self):
        self.logger = FakeLogger()

    def test_ftp_upload_progress(self):
        cbk = FtpUploadProgress(self.logger, 0)
        cbk(b'four')
        self.assertIn('(bytes sent: 4 of unknown, upload elapsed time: ', self.logger.messages[-1])

    def test_ftp_download_progress(self):
        cbk = FtpDownloadProgress(self.logger, 0)
        cbk(b'four')
        self.assertIn('(bytes downloaded: 4 of unknown, download elapsed time: ',
                      self.logger.messages[-1])

    def test_boto_upload_progress(self):
        cbk = BotoUploadProgress(self.logger, 0)
        cbk(5, 10)
        self.assertIn('(bytes sent: 5 of 10, upload elapsed time: ', self.logger.messages[-1])
