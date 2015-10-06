import unittest
from exporters.export_managers.base_exporter import BaseExporter
from exporters.export_managers.basic_exporter import BasicExporter
from exporters.export_managers.bypass import BaseBypass
from exporters.exporter_config import ExporterConfig
from exporters.readers.random_reader import RandomReader
from exporters.transform.no_transform import NoTransform
from exporters.writers.console_writer import ConsoleWriter


class BaseExportManagerTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'formatter': {
                    'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
                    'options': {
                        'show_titles': True,
                        'columns': ['city', 'country_code'],
                        'titles': ['City', 'CC']
                    }
                }
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 1
                }
            },
            'filter': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {

                }
            },
            'filter_after': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {

                }
            },
            'transform': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {

                }
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {

                }
            },
            'persistence': {
                'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                'options': {
                    'file_path': '/tmp/'
                }
            }
        }
        self.exporter = BaseExporter(self.config)

    def test_iteration(self):
        self.assertIs(self.exporter._run_pipeline_iteration(), None)
        self.exporter._clean_export_job()

    def test_full_export(self):
        self.assertIs(self.exporter._handle_export_exception(Exception()), None)
        self.assertIs(self.exporter.export(), None)

    def test_bypass(self):
        with self.assertRaises(NotImplementedError):
            self.exporter.bypass_exporter(BaseBypass(ExporterConfig(self.config)))
        self.exporter._clean_export_job()


class BasicExportManagerTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'formatter': {
                    'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
                    'options': {
                        'show_titles': True,
                        'columns': ['city', 'country_code'],
                        'titles': ['City', 'CC']
                    }
                },
                "notifications": [
                    {
                        "name": "exporters.notifications.s3_mail_notifier.S3MailNotifier",
                        "options": {
                            "client_name": "someclient",
                            "team_mails": [
                                "teammail@mail"
                            ],
                            "client_mails": [
                                "clientmail@mail"
                            ],
                            "aws_login": "somekey",
                            "aws_key": "someotherkey"
                        }
                    }
                ]
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'filter': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {

                }
            },
            'filter_after': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {

                }
            },
            'transform': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {

                }
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {

                }
            },
            'persistence': {
                'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                'options': {
                    'file_path': '/tmp/'
                }
            }
        }
        self.exporter = BasicExporter(self.options)

    def tearDown(self):
        self.exporter._clean_export_job()

    def test_parses_the_options_and_loads_pipeline_items(self):
        self.assertTrue(isinstance(self.exporter.reader, RandomReader))
        self.assertTrue(isinstance(self.exporter.writer, ConsoleWriter))
        self.assertTrue(isinstance(self.exporter.transform, NoTransform))

    def test_from_file_configuration(self):
        test_manager = BasicExporter.from_file_configuration(
            './tests/data/basic_config.json')
        self.assertIsInstance(test_manager, BasicExporter)
        test_manager._clean_export_job()

    def test_secure_option(self):
        options = self.exporter.secure_configuration['exporter_options']['notifications'][0]['options']
        self.assertNotIn('aws_login', options)
        self.assertNotIn('aws_key', options)