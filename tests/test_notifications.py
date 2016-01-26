import datetime
import json
import unittest

import mock
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.ses_mail_notifier import (DEFAULT_MAIN_FROM,
                                                       InvalidMailProvided,
                                                       SESMailNotifier)
from exporters.notifications.webhook_notifier import WebhookNotifier


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
        self.notifier = BaseNotifier(self.options)

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

        test_notifier = BaseNotifier(options)
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
                        'name': 'exporters.notifications.s3_mail_notifier.SESMailNotifier',
                        'options':
                            {
                                'team_mails': ['test@test.com'],
                                'client_mails': ['test@test.com'],
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
        self.job_info = self._create_stats()
        self.notifier = SESMailNotifier(self.options['exporter_options']['notifications'][0])

    def _create_stats(self):
        return {
            'configuration': self.options,
            'items_count': 2,
            'accurate_items_count': True,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }

    @mock.patch('boto.connect_ses')
    def test_start_dump(self, mock_ses):
        self.notifier.notify_start_dump(['test@test.com'], self._create_stats())
        expected_send_email = mock.call.send_email(
            DEFAULT_MAIN_FROM,
            'Started Customer export job',
            u'\nExport job started with following parameters:\n\n\nUsing: somewriter',
            mock.ANY
        )
        self.assertEquals([expected_send_email], mock_ses.return_value.mock_calls)

    @mock.patch('boto.connect_ses')
    def test_complete_dump(self, mock_ses):
        self.notifier.notify_complete_dump(['test@test.com'], self._create_stats())

        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Customer export job finished',
            u'\nExport job finished successfully\n\n\nTotal records exported: 2\n\n'
            'If you have any questions or concerns about the data you have received, email us at help@scrapinghub.com.\n',
            mock.ANY
        )

    @mock.patch('boto.connect_ses')
    def test_complete_dump_no_accurate_count(self, mock_ses):
        stats = self._create_stats()
        stats['accurate_items_count'] = False
        self.notifier.notify_complete_dump(['test@test.com'], stats)
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Customer export job finished',
            u'\nExport job finished successfully\n\n\n\n'
            'If you have any questions or concerns about the data you have received, email us at help@scrapinghub.com.\n',
            mock.ANY
        )

    @mock.patch('boto.connect_ses')
    def test_failed_dump(self, mock_ses):
        # TODO: check where is sending to
        # TODO: check job key
        self.notifier.notify_failed_job('REASON', 'STACKTRACE', ['test@test.com'], self._create_stats())
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Failed export job for Customer',
            u'\nExport job failed with following error:\n\n'
            u'REASON\n\n'
            u'Stacktrace:\nSTACKTRACE\n\n'
            u'Configuration:\n' + json.dumps(self.options),
            mock.ANY
        )

    @mock.patch('boto.connect_ses')
    def test_failed_dump(self, mock_ses):
        # TODO: check where is sending to
        # TODO: check job key
        self.notifier.notify_failed_job('REASON', 'STACKTRACE', ['test@test.com'], self._create_stats())
        mock_ses.return_value.send_email.assert_called_once_with(
            DEFAULT_MAIN_FROM,
            'Failed export job for Customer',
            u'\nExport job failed with following error:\n\n'
            u'REASON\n\n'
            u'Stacktrace:\nSTACKTRACE\n\n'
            u'Configuration:\n' + json.dumps(self.options),
            mock.ANY
        )

    @mock.patch('boto.connect_ses')
    def test_notify_team(self, mock_connect):
        send_mail_mock = mock.Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_team('Test mail')

    @mock.patch('boto.connect_ses')
    def test_notify_clients(self, mock_connect):
        send_mail_mock = mock.Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_clients('Test mail')

    @mock.patch('boto.connect_ses')
    def test_notify_daily(self, mock_connect):
        self.notifier.daily = True
        send_mail_mock = mock.Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    @mock.patch('boto.connect_ses')
    def test_notify_copy_key(self, mock_connect):
        self.notifier.copy_key = 'some copy key'
        send_mail_mock = mock.Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    @mock.patch('os.environ')
    @mock.patch('boto.connect_ses')
    def test_notify_shub_jobkey(self, mock_connect, mock_env):
        mock_env.return_value = {'SHUB_JOBKEY': 'somekey'}
        self.notifier.copy_key = 'some copy key'
        send_mail_mock = mock.Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_failed_job('Test fail reason', '', ['test@test.com'], self.job_info)

    def test_invalid_mails(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'notifications': [
                    {
                        'name': 'exporters.notifications.s3_mail_notifier.S3MailNotifier',
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
            SESMailNotifier(options['exporter_options']['notifications'][0])


class WebhookNotifierTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'notifications': [
                    {
                        'name': 'exporters.notifications.webhook_notifier.WebhookNotifier',
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
        self.job_info = {
            'configuration': self.options,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.notifier = WebhookNotifier(self.options['exporter_options']['notifications'][0])

    @mock.patch('requests.post')
    def test_start_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_start_dump([], self.job_info)

    @mock.patch('requests.post')
    def test_completed_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_complete_dump([], self.job_info)

    @mock.patch('requests.post')
    def test_failed_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_failed_job('', '', info=self.job_info)
