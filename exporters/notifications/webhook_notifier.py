import json
import logging
import requests
from exporters.notifications.base_notifier import BaseNotifier
from retrying import retry
import datetime


def _datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return str(obj)


STARTED_JOB = 'STARTED'
COMPLETED_JOB = 'COMPLETED'
FAILED_JOB = 'FAILED'


class WebhookNotifier(BaseNotifier):
    """
    Performs a POST request to provided endpoints

    Needed parameters:

        - endpoints (list)
            Endpoints waiting for a start notification

    """
    def __init__(self, options):
        # List of options
        self.parameters = {
            'endpoints': {'type': list, 'default': []}
        }

        super(WebhookNotifier, self).__init__(options)
        self.options = options['options']
        self.endpoints = self.options.get('endpoints', [])

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
                logging.log(logging.WARNING, 'There was an error running export webhook to endpoint {}. '
                                             'Exception: {!r}'.format(url, str(e)))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def _make_request(self, url, payload):
        headers = {'Content-type': 'application/json'}
        requests.post(url, data=payload, headers=headers)
