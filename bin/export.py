#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script wraps exporters launching logic, and should be used to make export jobs both from localhost and from dash.
"""

from __future__ import print_function
import json
import os
import requests
from requests.auth import HTTPBasicAuth
from retrying import retry
import yaml
from exporters.export_managers.unified_exporter import UnifiedExporter
import logging


EXPORTER_API = 'https://datahub-exports-api.scrapinghub.com/exports'
DS_JOBS_URL = 'https://staging.scrapinghub.com'
DS_PROJECT_NUMBER = 7389


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apikey', help='Valid apikey', default=os.getenv('SHUB_APIKEY', None))
    parser.add_argument('--useapi', action='store_true', help='Exporters api endpoint')
    parser.add_argument('--label', help='Export label', default='export-job')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--resume', help='Resume a preexisting export job')
    group.add_argument('--config', help='Configuration file or url')

    args = parser.parse_args()
    if args.config and args.config.startswith('http'):
        if not args.apikey:
            raise ValueError('--apikey or SHUB_APIKEY environment variable missing')
    return args


def configuration_from_file(path):
    return yaml.safe_load(open(path).read())


@retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
def _make_request(url, apikey, data=None):
    method = 'POST' if data else 'GET'
    auth = HTTPBasicAuth(apikey, '')
    headers = {'Content-type': 'application/json'}
    return requests.request(method, url, auth=auth, headers=headers, data=data)


def configuration_from_url(url, apikey):
    return yaml.safe_load(_make_request(url, apikey).text)


def schedule_job(configuration, apikey, label):
    config = {'export_configuration': configuration, 'label': label}
    response = _make_request(url=EXPORTER_API, apikey=apikey, data=json.dumps(config))
    return json.loads(response.text)['job_id']


def run(args):
    if args.useapi:
        if args.config.startswith('http'):
            configuration = configuration_from_url(args.config, args.apikey)
        else:
            configuration = configuration_from_file(args.config)
        job_id = schedule_job(configuration, args.apikey, args.label)
        job_url = '{}/p/{}/job/{}'.format(DS_JOBS_URL, DS_PROJECT_NUMBER, '/'.join(job_id.split('/')[1:]))
        logging.log(logging.INFO, 'Job scheduled in: {}'.format(job_url))
    else:
        if args.resume:
            exporter = UnifiedExporter.from_persistence_configuration(args.resume)
        elif args.config.startswith('http'):
            exporter = UnifiedExporter.from_url_configuration(args.config, args.apikey)
        else:
            exporter = UnifiedExporter.from_file_configuration(args.config)
        exporter.export()


if '__main__' == __name__:
    args = parse_args()
    run(args)
