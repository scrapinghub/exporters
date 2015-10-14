import json
import unittest
import datetime
from mock import patch, Mock
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.ses_mail_notifier import SESMailNotifier, InvalidMailProvided
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
        self.job_info = {
            'configuration': self.options,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.notifier = SESMailNotifier(self.options['exporter_options']['notifications'][0])

    @patch('boto.connect_ses')
    def test_start_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_start_dump(['test@test.com'], self.job_info)

    def test_generate_start_body(self):
        expected_body = "{name} dump started with following parameters:\n\n"
        expected_body += 'Used writer: {writer}\n'
        expected_body = expected_body.format(
            name='basic_export_manager',
            writer='somewriter',
        )
        self.assertEqual(self.notifier._generate_start_dump_body(self.job_info), expected_body)

    @patch('boto.connect_ses')
    def test_complete_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    def test_generate_complete_body(self):
        expected_body = "{name} dump finished with following parameters:\n\n"
        expected_body += 'Used writer: {writer}\n'
        expected_body += 'Total records dumped: {total}\n\n'
        expected_body += 'If you have any questions or concerns about the data you have received, ' \
                'please email us at help@scrapinghub.com.\n'
        expected_body = expected_body.format(
            name='basic_export_manager',
            writer='somewriter',
            total=0,
        )
        self.assertEqual(self.notifier._generate_complete_dump_body(self.job_info), expected_body)

    @patch('boto.connect_ses')
    def test_failed_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_failed_job('Test fail reason', '', ['test@test.com'], self.job_info)

    def test_generate_failed_body(self):
        expected_body = '{} dump failed with following error:'.format('basic_export_manager')
        expected_body += '\n\nTest fail reason\n'
        expected_body += '\nStacktrace: \n'
        expected_body += '\n\nConfiguration: \n' + json.dumps(self.options)
        self.assertEqual(self.notifier._generate_failed_job_body('Test fail reason', '', self.job_info), expected_body)

    @patch('boto.connect_ses')
    def test_notify_team(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_team('Test mail')

    @patch('boto.connect_ses')
    def test_notify_clients(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_clients('Test mail')

    @patch('boto.connect_ses')
    def test_notify_daily(self, mock_connect):
        self.notifier.daily = True
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    @patch('boto.connect_ses')
    def test_notify_copy_key(self, mock_connect):
        self.notifier.copy_key = 'some copy key'
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    @patch('os.environ')
    @patch('boto.connect_ses')
    def test_notify_shub_jobkey(self, mock_connect, mock_env):
        mock_env.return_value = {'SHUB_JOBKEY': 'somekey'}
        self.notifier.copy_key = 'some copy key'
        send_mail_mock = Mock()
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

    @patch('requests.post')
    def test_start_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_start_dump([], self.job_info)

    @patch('requests.post')
    def test_completed_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_complete_dump([], self.job_info)

    @patch('requests.post')
    def test_failed_dump(self, mock_request):
        mock_request.return_value = True
        self.notifier.notify_failed_job('', '', info=self.job_info)
