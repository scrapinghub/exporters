import json
import logging
from exporters.notifications.base_notifier import BaseNotifier
from exporters.default_retries import retry_short
import datetime
from exporters.utils import str_list


def _datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return str(obj)


STARTED_JOB = 'STARTED'
COMPLETED_JOB = 'COMPLETED'
FAILED_JOB = 'FAILED'


class WebhookNotifier(BaseNotifier):
    """
    Performs a POST request to provided endpoints

        - endpoints (list)
            Endpoints waiting for a start notification

    """
    supported_options = {
        'endpoints': {'type': str_list, 'default': []}
    }

    def __init__(self, *args, **kwargs):
        super(WebhookNotifier, self).__init__(*args, **kwargs)
        self.endpoints = self.read_option('endpoints', [])

    def notify_start_dump(self, receivers=None, info=None):
        payload = self._get_info(info, STARTED_JOB)
        self._send_info(payload)

    def notify_complete_dump(self, receivers=None, info=None):
        payload = self._get_info(info, COMPLETED_JOB)
        self._send_info(payload)

    def notify_failed_job(self, msg, stack_trace, receivers=None, info=None):
        payload = self._get_info(info, FAILED_JOB, msg=msg, stack_trace=stack_trace)
        self._send_info(payload)

    def _get_info(self, info, state, msg=None, stack_trace=None):
        if info is None:
            info = {}
        info['job_status'] = state
        if msg:
            info['msg'] = msg
        if stack_trace:
            info['stack_trace'] = stack_trace
        return json.dumps(info, default=_datetime_serializer)

    def _send_info(self, payload):
        for url in self.endpoints:
            try:
                self._make_request(url, payload)
            except Exception as e:
                logging.warn('There was an error running export webhook to endpoint {}. '
                             'Exception: {!r}'.format(url, str(e)))

    @retry_short
    def _make_request(self, url, payload):
        import requests
        headers = {'Content-type': 'application/json'}
        requests.post(url, data=payload, headers=headers)
