#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import tempfile
from pydrive.auth import GoogleAuth


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-secret', help='Client Secret file', required=True)
    parser.add_argument('--dest', help='Credentials File Destination', default=tempfile.mkdtemp())
    args = parser.parse_args()
    return args

def run(args):
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(args.client_secret)
    gauth.LocalWebserverAuth()
    credentials_file = os.path.join(args.dest, 'gdrive-credentials.json')
    gauth.SaveCredentialsFile(credentials_file)
    print('Credentials file saved to {}'.format(credentials_file))

if '__main__' == __name__:
    args = parse_args()
    run(args)
