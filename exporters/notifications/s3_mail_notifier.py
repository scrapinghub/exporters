import json
import os
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.receiver_groups import CLIENTS, TEAM

DEFAULT_MAIN_FROM = 'Scrapinghub data services <dataservices@scrapinghub.com>'


class S3MailNotifier(BaseNotifier):
    """
    Sends emails using aws mail service

        - team_mails (list)
            List of the mails from the team members

        - client_mails (list)
            List of client mails

        - aws_login (str)
            Aws acces key

        - aws_key (str)
            Aws secret access key
    """
    def __init__(self, options):
        # List of options
        self.supported_options = {
            'team_mails': {'type': list, 'default': []},
            'client_mails': {'type': list, 'default': []},
            'aws_login': {'type': basestring, 'secret': True},
            'aws_key': {'type': basestring, 'secret': True}
        }

        super(S3MailNotifier, self).__init__(options)
        self.options = options['options']
        self.team_mails = self.options['team_mails']
        self.client_mails = self.options['client_mails']

    def notify_team(self, msg):
        self._send_email(self.team_mails, 'Notification', msg)

    def notify_clients(self, msg):
        self._send_email(self.client_mails, 'Notification', msg)

    def notify_start_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        mails = self._get_mails(receivers)
        self._notify_start_dump(mails, info)

    def notify_complete_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        mails = self._get_mails(receivers)
        self._notify_complete_dump(mails, info)


    def notify_failed_job(self, msg, stack_trace, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        mails = self._get_mails(receivers)
        self._notify_failed_job(msg, stack_trace, mails, info)

    def _send_email(self, mails, subject, body):
        import boto
        ses = boto.connect_ses(self.options['aws_login'], self.options['aws_key'])
        ses.send_email(self.options.get('mail_from', DEFAULT_MAIN_FROM), subject, body, mails)

    def _get_mails(self, receivers):
        mails = []
        for receiver in receivers:
            if receiver == CLIENTS:
                mails.extend(self.client_mails)
            elif receiver == TEAM:
                mails.extend(self.team_mails)
        return mails

    def _generate_start_dump_body(self, info):
        body = "{name} dump started with following parameters:\n\n"
        body += 'Used writer: {writer}\n'

        body = body.format(
            name=info.get('script_name', 'dump_job'),
            writer=info['configuration']['writer']['name'],
        )
        return body

    def _notify_start_dump(self, mails, info=None):
        if info is None:
            info = {}
        body = self._generate_start_dump_body(info)
        subject = 'Started {client} {name} dump'.format(client=info.get('client_name', 'Customer'), name=info.get('script_name', 'dump_job'))
        self._send_email(mails, subject, body)

    def _generate_complete_dump_body(self, info):
        body = "{name} dump finished with following parameters:\n\n"
        body += 'Used writer: {writer}\n'
        body += 'Total records dumped: {total}\n\n'
        body += 'If you have any questions or concerns about the data you have received, ' \
                'please email us at help@scrapinghub.com.\n'
        body = body.format(
            name=info.get('script_name', 'dump_job'),
            writer=info['configuration']['writer']['name'],
            total=info.get('items_count', 0),
        )
        return body

    def _notify_complete_dump(self, mails, info=None):
        if info is None:
            info = {}
        body = self._generate_complete_dump_body(info)
        subject = '{client} {name} dump completed'.format(client=info.get('client_name', 'Customer'), name=info.get('script_name', 'dump_job'))
        self._send_email(mails, subject, body)


    def _generate_failed_job_body(self, msg, stack_trace, info):
        body = '{} dump failed with following error:\n\n'.format(info.get('script_name', 'dump_job'))
        if 'SHUB_JOBKEY' in os.environ:
            pid, jobid = os.environ['SHUB_JOBKEY'].split('/', 1)
            msg = 'Job ID: <a href="https://dash.scrapinghub.com/p/{pid}/job/{jobid}">{jobkey}</a>\n\n'.format(
                pid=pid, jobid=jobid,
                jobkey=os.environ['SHUB_JOBKEY']
            ) + msg
        msg += '\n\nStacktrace: \n' + stack_trace
        msg += '\n\nConfiguration: \n' + json.dumps(info.get('configuration'))
        body = body + msg
        return body

    def _notify_failed_job(self, msg, stack_trace, mails, info=None):
        if info is None:
            info = {}
        body = self._generate_failed_job_body(msg, stack_trace, info)
        subject = '{name} dump for {client} failed.'.format(client=info.get('client_name', 'Customer'), name=info.get('script_name', 'dump_job'))
        self._send_email(mails, subject, body)
