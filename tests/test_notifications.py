import datetime
import json
import unittest

import mock
from ozzy.meta import ExportMeta
from ozzy.notifications.base_notifier import BaseNotifier
from ozzy.notifications.receiver_groups import CLIENTS, TEAM
from ozzy.notifications.ses_mail_notifier import (DEFAULT_MAIN_FROM,
                                                  InvalidMailProvided,
                                                  SESMailNotifier)
from ozzy.notifications.webhook_notifier import WebhookNotifier
from tests.utils import environment


class BaseNotifierTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'options': {

            }
        }
        self.notifier = BaseNotifier(self.options, {})

    def test_raise_exception_start_dump(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_start_dump([])

    def test_raise_exception_complete_dump(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_complete_dump([])

    def test_raise_exception_failed_job(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_failed_job('', '', [])

    def test_check_not_existing_required_supported_option(self):
        with self.assertRaises(Exception):
            test_notifier = self.notifier
            test_notifier.supported_options.append({'name': 'test', 'type': basestring})
            test_notifier.check_options()

    def test_check_not_required_supported_option(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'options': {

            }
        }

        test_notifier = BaseNotifier(options, {})
        test_notifier.supported_options['test'] = {'type': int, 'default': 5}
        test_notifier.check_options()

    def test_check_bad_type_required_supported_option(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'options': {
                "test": 100
            }
        }
        with self.assertRaises(Exception):
            test_notifier = BaseNotifier(options)
            test_notifier.supported_options.append({'name': 'test', 'type': basestring})
            test_notifier.check_options()


class SESMailNotifierTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'notifications': [
                    {
                        'name': 'ozzy.notifications.s3_mail_notifier.SESMailNotifier',
                        'options':
                            {
                                'team_mails': ['team@example.com'],
                                'client_mails': ['client@example.com'],
                                'access_key': 'somelogin',
                                'secret_key': 'somekey'
                            }
                    }
                ]
            },
            'writer': {
                'name': 'somewriter',
                'options': {
                    'some_option': 'some_value',
                    'bucket': 'SOMEBUCKET',
                    'filebase': 'FILEBASE',
                }
            }

        }
        self.meta = ExportMeta(self.options)
        self.meta.per_module['writer']['items_count'] = 2
        self.notifier = SESMailNotifier(
            self.options['exporter_options']['notifications'][0], self.meta)

    @mock.patch('boto.connect_ses')
    def test_start_dump(self, mock_ses):
        self.notifier.notify_start_dump([CLIENTS, TEAM])
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Started Customer export job',
            u'\nExport job started with following parameters:\n\nWriter: somewriter'
            u'\nBucket: SOMEBUCKET\nFilebase: FILEBASE',
            ['client@example.com', 'team@example.com']
        )

    @mock.patch('boto.connect_ses')
    def test_notify_with_custom_emails(self, mock_ses):
        self.notifier.notify_start_dump(['test@test.com'])
        mock_ses.return_value.send_email.assert_called_once_with(
            mock.ANY,
            mock.ANY,
            mock.ANY,
            ['test@test.com']
        )

    @mock.patch('boto.connect_ses')
    def test_complete_dump(self, mock_ses):
        self.notifier.notify_complete_dump([CLIENTS, TEAM])
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Customer export job finished',
            u'\nExport job finished successfully.\n\nTotal records exported: 2.\n\n'
            'If you have any questions or concerns about the data you have received, '
            'email us at dataservices@scrapinghub.com.\n',
            ['client@example.com', 'team@example.com']
        )

    @mock.patch('boto.connect_ses')
    def test_complete_dump_no_accurate_count(self, mock_ses):
        self.meta.accurate_items_count = False
        self.notifier.notify_complete_dump(['test@test.com'])
        self.meta.accurate_items_count = False
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Customer export job finished',
            u'\nExport job finished successfully.\n\n\n\n'
            'If you have any questions or concerns about the data you have received, '
            'email us at dataservices@scrapinghub.com.\n',
            mock.ANY
        )

    @mock.patch('boto.connect_ses')
    def test_failed_dump(self, mock_ses):
        self.notifier.notify_failed_job('REASON', 'STACKTRACE', ['test@test.com'])
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Failed export job for Customer',
            u'\nExport job failed with following error:\n\n'
            u'REASON\n\n'
            u'Stacktrace:\nSTACKTRACE\n\n'
            u'Configuration:\n' + json.dumps(self.options),
            ['test@test.com']
        )

    @mock.patch('boto.connect_ses')
    def test_failed_dump_in_scrapy_cloud(self, mock_ses):
        with environment(dict(SHUB_JOBKEY='10804/1/12')):
            self.notifier.notify_failed_job('REASON', 'STACKTRACE', ['test@test.com'])

        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Failed export job for Customer',
            u'\nExport job failed with following error:\n\n'
            u'REASON\n\n'
            u'Job key: 10804/1/12\n'
            u'Job: https://dash.scrapinghub.com/p/10804/job/1/12\n\n'
            u'Stacktrace:\nSTACKTRACE\n\n'
            u'Configuration:\n' + json.dumps(self.options),
            mock.ANY
        )

    def test_invalid_mails(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'notifications': [
                    {
                        'name': 'ozzy.notifications.s3_mail_notifier.S3MailNotifier',
                        'options':
                            {
                                'team_mails': ['badmail'],
                                'client_mails': [],
                                'access_key': 'somelogin',
                                'secret_key': 'somekey'
                            }
                    }
                ]
            },
            'writer': {
                'name': 'somewriter',
                'options': {
                    'some_option': 'some_value'
                }
            }
        }
        with self.assertRaises(InvalidMailProvided):
            SESMailNotifier(options['exporter_options']['notifications'][0], {})


class WebhookNotifierTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'notifications': [
                    {
                        'name': 'ozzy.notifications.webhook_notifier.WebhookNotifier',
                        'options':
                            {
                                'endpoints': ['http://test.com']
                            }
                    }
                ]
            },
            'writer': {
                'name': 'somewriter',
                'options': {
                    'some_option': 'some_value'
                }
            }

        }
        self.meta = {
            'configuration': self.options,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.notifier = WebhookNotifier(
            self.options['exporter_options']['notifications'][0], self.meta)

    @mock.patch('requests.post')
    def test_start_dump(self, mock_request):
        # TODO: make this test actually test something
        self.notifier.notify_start_dump([])

    @mock.patch('requests.post')
    def test_completed_dump(self, mock_request):
        # TODO: make this test actually test something
        self.notifier.notify_complete_dump([])

    @mock.patch('requests.post')
    def test_failed_dump(self, mock_request):
        # TODO: make this test actually test something
        self.notifier.notify_failed_job('', '')
