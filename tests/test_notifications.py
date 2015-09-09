import unittest
import datetime
from mock import patch, Mock
from exporters.export_managers.settings import Settings
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.s3_mail_notifier import S3MailNotifier
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
        self.settings = Settings(self.options['exporter_options'])
        self.notifier = BaseNotifier(self.options, self.settings)

    def test_raise_exception_start_dump(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_start_dump([])

    def test_raise_exception_complete_dump(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_complete_dump([])

    def test_raise_exception_failed_job(self):
        with self.assertRaises(NotImplementedError):
            self.notifier.notify_failed_job('', '', [])

    def test_check_not_existing_required_parameter(self):
        with self.assertRaises(Exception):
            test_notifier = self.notifier
            test_notifier.requirements.append({'name': 'test', 'type': basestring, 'required': True})
            test_notifier.check_options()

    def test_check_not_required_parameter(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'options': {

            }
        }

        test_notifier = BaseNotifier(options, self.settings)
        test_notifier.requirements['test'] = {'type': int, 'required': False}
        test_notifier.check_options()

    def test_check_bad_type_required_parameter(self):
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
            test_notifier = BaseNotifier(options, self.settings)
            test_notifier.requirements.append({'name': 'test', 'type': basestring, 'required': True})
            test_notifier.check_options()


class S3MailNotifierTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'NOTIFICATIONS': [
                    {
                        'name': 'exporters.notifications.s3_mail_notifier.S3MailNotifier',
                        'options':
                            {
                                'team_mails': ['test@test.com'],
                                'client_mails': ['test@test.com'],
                                'aws_login': 'somelogin',
                                'aws_key': 'somekey'
                            }
                    }
                ]
            },
            'writer': {}

        }
        self.job_info = {
            'configuration': self.options,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.settings = Settings(self.options['exporter_options'])
        self.notifier = S3MailNotifier(self.options['exporter_options']['NOTIFICATIONS'][0], self.settings)

    @patch('boto.connect_ses')
    def test_start_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_start_dump(['test@test.com'], self.job_info)

    @patch('boto.connect_ses')
    def test_complete_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_complete_dump(['test@test.com'], self.job_info)

    @patch('boto.connect_ses')
    def test_failed_dump(self, mock_connect):
        send_mail_mock = Mock()
        send_mail_mock.send_email.return_value = True
        mock_connect.return_value = send_mail_mock
        self.notifier.notify_failed_job('Test fail reason', '', ['test@test.com'], self.job_info)

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


class WebhookNotifierTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                'NOTIFICATIONS': [
                    {
                        'name': 'exporters.notifications.webhook_notifier.WebhookNotifier',
                        'options':
                            {
                                'endpoints': ['http://test.com']
                            }
                    }
                ]
            },
            'writer': {}

        }
        self.job_info = {
            'configuration': self.options,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.settings = Settings(self.options['exporter_options'])
        self.notifier = WebhookNotifier(self.options['exporter_options']['NOTIFICATIONS'][0], self.settings)

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
