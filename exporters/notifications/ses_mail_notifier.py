import json
import os
import re
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from jinja2 import Template


DEFAULT_MAIN_FROM = 'Scrapinghub data services <dataservices@scrapinghub.com>'


def render(template_text, **data):
    template = Template(template_text)
    template.globals['as_json'] = json.dumps
    return template.render(**data)


def _render_start_dump_email(**data):
    subject_tmpl = 'Started {{ client }} export job'
    body_tmpl = """
Export job started with following parameters:

{% set writer_name = configuration.writer.name.split('.')[-1] %}
Using: {{ writer_name }}
"""
    return render(subject_tmpl, **data), render(body_tmpl, **data)


def _render_complete_dump_email(**data):
    subject_tmpl = '{{ client }} export job finished'
    body_tmpl = """
Export job finished successfully

{% if accurate_items_count %}
Total records exported: {{ items_count }}
{%- endif %}

If you have any questions or concerns about the data you have received, email us at help@scrapinghub.com.\n
"""
    return render(subject_tmpl, **data), render(body_tmpl, **data)


def _render_failed_job_email(**data):
    subject_tmpl = 'Failed export job for {{ client }}'
    body_tmpl = """
Export job failed with following error:

{{ reason }}

Stacktrace:
{{ stacktrace }}

Configuration:
{{ as_json(configuration) }}
"""
    return render(subject_tmpl, **data), render(body_tmpl, **data)


class InvalidMailProvided(Exception):
    pass


class SESMailNotifier(BaseNotifier):
    """
    Sends email notifications using aws mail service

        - team_mails (list)
            List of the mails from the team members

        - client_mails (list)
            List of client mails

        - access_key (str)
            AWS access key

        - secret_key (str)
            AWS secret access key
    """
    def __init__(self, options):
        self.supported_options = {
            'team_mails': {'type': list, 'default': []},
            'client_mails': {'type': list, 'default': []},
            'access_key': {'type': basestring, 'env_fallback': 'EXPORTERS_MAIL_AWS_ACCESS_KEY'},
            'secret_key': {'type': basestring, 'env_fallback': 'EXPORTERS_MAIL_AWS_SECRET_KEY'},
            'client_name': {'type': basestring, 'default': 'Customer'},
        }

        super(SESMailNotifier, self).__init__(options)
        self.options = options['options']
        self.team_mails = self.options['team_mails']
        self.client_mails = self.options['client_mails']
        self.client_name = self.read_option('client_name')
        self._check_mails()

    def _check_mails(self):
        for mail in self.team_mails + self.client_mails:
            if not re.match('.+@.+', mail):
                raise InvalidMailProvided()

    def notify_team(self, msg):
        self._send_email(self.team_mails, 'Notification', msg)

    def notify_clients(self, msg):
        self._send_email(self.client_mails, 'Notification', msg)

    def notify_start_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        info = info or None
        mails = self._get_mails(receivers)
        subject, body = _render_start_dump_email(client=self.client_name, **info)
        self._send_email(mails, subject, body)

    def notify_complete_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        mails = self._get_mails(receivers)
        subject, body = _render_complete_dump_email(client=self.client_name, **info)
        self._send_email(mails, subject, body)

    def notify_failed_job(self, msg, stack_trace, receivers=None, info=None):
        receivers = receivers or []
        info = info or {}
        mails = self._get_mails(receivers)
        body = self._generate_failed_job_body(msg, stack_trace, info)
        subject, body = _render_failed_job_email(
            client=self.client_name,
            reason=msg,
            stacktrace=stack_trace,
            **info
        )
        self._send_email(mails, subject, body)

    def _send_email(self, mails, subject, body):
        import boto
        ses = boto.connect_ses(self.read_option('access_key'), self.read_option('secret_key'))
        ses.send_email(self.options.get('mail_from', DEFAULT_MAIN_FROM), subject, body, mails)

    def _get_mails(self, receivers):
        mails = []
        for receiver in receivers:
            if receiver == CLIENTS:
                mails.extend(self.client_mails)
            elif receiver == TEAM:
                mails.extend(self.team_mails)
        return mails

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
