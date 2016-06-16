import json
import os
import re

from exporters.default_retries import retry_short
from exporters.notifications.base_notifier import BaseNotifier
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from exporters.utils import str_list


DEFAULT_MAIN_FROM = 'Scrapinghub data services <dataservices@scrapinghub.com>'


def get_scrapy_cloud_link(jobkey):
    if not jobkey:
        return ''
    proj_id, remainder = jobkey.split('/', 1)
    return 'https://dash.scrapinghub.com/p/%s/job/%s' % (proj_id, remainder)


def render(template_text, **data):
    from jinja2 import Template
    template = Template(template_text)
    template.globals['as_json'] = json.dumps
    template.globals['job_link'] = get_scrapy_cloud_link
    return template.render(**data)


def _render_start_dump_email(**data):
    subject_tmpl = 'Started {{ client }} export job'
    body_tmpl = """
Export job started with following parameters:

{% set writer_name = configuration.writer.name.split('.')[-1] -%}
{% set writer_options = configuration.writer.options -%}
Writer: {{ writer_name }}
{%- if writer_options.bucket %}
Bucket: {{ writer_options.bucket }}
{%- endif -%}
{%- if writer_options.filebase %}
Filebase: {{ writer_options.filebase }}
{%- endif -%}
{%- if writer_options.host %}
Host: {{ writer_options.host }}
{%- endif -%}
{%- if writer_options.port %}
Port: {{ writer_options.port }}
{%- endif -%}
{%- if writer_options.port %}
Port: {{ writer_options.port }}
{%- endif -%}
{%- if writer_options.email %}
Email: {{ writer_options.email }}
{%- endif -%}
{%- if writer_options.endpoint_url %}
Endpoint URL: {{ writer_options.endpoint_url }}
{%- endif -%}
{%- if writer_options.collection_name and writer_options.project_id %}
Target Hubstorage collection URL: https://dash.scrapinghub.com/p/{{ writer_options.project_id }}/collections/s/{{ writer_options.collection_name }}
{%- endif -%}
"""  # noqa
    return render(subject_tmpl, **data), render(body_tmpl, **data)


def _render_complete_dump_email(**data):
    subject_tmpl = '{{ client }} export job finished'
    body_tmpl = """
Export job finished successfully.

{% if accurate_items_count -%}
Total records exported: {{ writer.items_count }}.
{%- endif %}
{%- set writer_options = configuration.writer.options -%}
{%- if writer_options.collection_name and writer_options.project_id %}
Target Hubstorage collection URL: https://dash.scrapinghub.com/p/{{ writer_options.project_id }}/collections/s/{{ writer_options.collection_name }}
{%- endif %}

If you have any questions or concerns about the data you have received, email us at dataservices@scrapinghub.com.\n
"""  # noqa
    return render(subject_tmpl, **data), render(body_tmpl, **data)


def _render_failed_job_email(**data):
    subject_tmpl = 'Failed export job for {{ client }}'
    body_tmpl = """
Export job failed with following error:

{{ reason }}
{% if jobkey %}
Job key: {{ jobkey }}
Job: {{ job_link(jobkey) }}
{%endif %}
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
    supported_options = {
        'team_mails': {'type': str_list, 'default': []},
        'client_mails': {'type': str_list, 'default': []},
        'access_key': {'type': basestring, 'env_fallback': 'EXPORTERS_MAIL_AWS_ACCESS_KEY'},
        'secret_key': {'type': basestring, 'env_fallback': 'EXPORTERS_MAIL_AWS_SECRET_KEY'},
        'client_name': {'type': basestring, 'default': 'Customer'},
    }

    def __init__(self, options, metadata):
        super(SESMailNotifier, self).__init__(options, metadata)
        self.team_mails = self.read_option('team_mails')
        self.client_mails = self.read_option('client_mails')
        self.client_name = self.read_option('client_name')
        self._check_mails()

    def _check_mails(self):
        for mail in self.team_mails + self.client_mails:
            if not re.match('.+@.+', mail):
                raise InvalidMailProvided()

    def notify_start_dump(self, receivers=None):
        receivers = receivers or []
        info = self.metadata.to_dict()
        mails = self._get_mails(receivers)
        subject, body = _render_start_dump_email(client=self.client_name, **info)
        self._send_email(mails, subject, body)

    def notify_complete_dump(self, receivers=None):
        receivers = receivers or []
        mails = self._get_mails(receivers)
        info = self.metadata.to_dict()
        subject, body = _render_complete_dump_email(client=self.client_name, **info)
        self._send_email(mails, subject, body)

    def notify_failed_job(self, msg, stack_trace, receivers=None):
        receivers = receivers or []
        mails = self._get_mails(receivers)
        info = self.metadata.to_dict()
        subject, body = _render_failed_job_email(
            client=self.client_name,
            reason=msg,
            stacktrace=stack_trace,
            jobkey=os.getenv('SHUB_JOBKEY'),
            **info
        )
        self._send_email(mails, subject, body)

    @retry_short
    def _send_email(self, mails, subject, body):
        if mails:
            import boto
            ses = boto.connect_ses(self.read_option('access_key'), self.read_option('secret_key'))
            ses.send_email(self.read_option('mail_from', DEFAULT_MAIN_FROM), subject, body, mails)

    def _get_mails(self, receivers):
        mails = []
        for receiver in receivers:
            if receiver == CLIENTS:
                mails.extend(self.client_mails)
            elif receiver == TEAM:
                mails.extend(self.team_mails)
            else:
                mails.append(receiver)
        return mails
