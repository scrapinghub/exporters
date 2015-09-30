#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script wraps exporters launching logic, and should be used to make export jobs both from localhost and from dash.
"""

from __future__ import print_function
import yaml
from exporters.export_managers.basic_exporter import BasicExporter


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--resume', help='Resume a preexisting export job')
    group.add_argument('--config', help='Configuration file path')
    args = parser.parse_args()
    return args


def run(args):
    if args.resume:
        exporter = BasicExporter.from_persistence_configuration(args.resume)
    else:
        exporter = BasicExporter.from_file_configuration(args.config)
    exporter.export()


if '__main__' == __name__:
    args = parse_args()
    run(args)
