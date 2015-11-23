#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Usage: python get_gdrive_credentials.py --client-secret PATH_TO_SECRET_FILE

The purpose of this script is to create google login credentials to be used by google drive
writer.

Expected workflow is:

1.- Get the client secret file. If you haven't one, please follow this tutorial:
https://developers.google.com/drive/web/quickstart/python

2.- Execute this script. It will open a browser tab in which you have to login with your
Google account. It will create a credentials file (file path will be printed).

3.- You can use the info contained in both files to configure a export using google
drive writer.
"""

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
