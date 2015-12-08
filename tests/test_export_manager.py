import os
import mock
import unittest
from exporters.export_managers.base_exporter import BaseExporter
from exporters.export_managers.basic_exporter import BasicExporter
from exporters.export_managers.bypass import BaseBypass, RequisitesNotMet
from exporters.readers.random_reader import RandomReader
from exporters.transform.no_transform import NoTransform
from exporters.writers.console_writer import ConsoleWriter
from .utils import valid_config_with_updates


def get_filename(path, persistence_id):
    return os.path.join(path, persistence_id)


def fail(*a, **kw):
    raise ValueError("fail")


class FakeBypass(BaseBypass):
    bypass_called = False
    should_call = True

    def meets_conditions(self):
        if not self.should_call:
            raise RequisitesNotMet

    def bypass(self):
        self.bypass_called = True


class BaseExportManagerTest(unittest.TestCase):
    def build_config(self, **kwargs):
        defaults = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 1
                }
            }
        }
        defaults.update(**kwargs)
        return valid_config_with_updates(defaults)

    def tearDown(self):
        if hasattr(self, 'exporter'):
            self.exporter.persistence.delete()

    def test_simple_export(self):
        self.exporter = exporter = BaseExporter(self.build_config())
        exporter.export()
        self.assertEquals(10, exporter.writer.items_count)

    def test_export_with_csv_formatter(self):
        config = self.build_config()
        config['exporter_options']['formatter'] = {
            'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
            'options': {
                'show_titles': True,
                'fields': ['city', 'country_code']
            }
        }
        self.exporter = exporter = BaseExporter(config)
        exporter.export()
        expected_count = 10 + 1  # FIXME: it's currently counting header as an item
        self.assertEquals(expected_count, exporter.writer.items_count)

    def test_bypass_should_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())
        bypass_instance = FakeBypass(exporter.config)
        exporter.bypass_cases = [bypass_instance]

        # when:
        exporter.export()

        # then:
        self.assertTrue(bypass_instance.bypass_called, "Bypass should have been called")

    def test_bypass_should_not_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())
        bypass_instance = FakeBypass(exporter.config)
        bypass_instance.should_call = False
        exporter.bypass_cases = [bypass_instance]

        # when:
        exporter.export()

        # then:
        self.assertFalse(bypass_instance.bypass_called, "Bypass should NOT have been called")

    @mock.patch('exporters.writers.ftp_writer.FTPWriter.write', new=fail)
    @mock.patch('exporters.export_managers.base_exporter.NotifiersList')
    def test_when_writing_only_on_flush_should_notify_job_failure(self, mock_notifier):
        config = self.build_config(
            writer={
                'name': 'exporters.writers.FTPWriter',
                'options': {
                    'host': 'ftp.invalid.com',
                    'filebase': '_',
                    'ftp_user': 'invaliduser',
                    'ftp_password': 'invalidpass',
                }
            },
        )
        self.exporter = exporter = BaseExporter(config)
        with self.assertRaisesRegexp(ValueError, "fail"):
            exporter.export()
        self.assertFalse(mock_notifier.return_value.notify_complete_dump.called,
                         "Should not notify a successful dump")
        self.assertTrue(mock_notifier.return_value.notify_failed_job.called,
                        "Should notify the job failure")


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
                        'fields': ['city', 'country_code']
                    }
                }
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {

                }
            }
        }

    def test_parses_the_options_and_loads_pipeline_items(self):
        exporter = BasicExporter(self.options)
        try:
            self.assertTrue(isinstance(exporter.reader, RandomReader))
            self.assertTrue(isinstance(exporter.writer, ConsoleWriter))
            self.assertTrue(isinstance(exporter.transform, NoTransform))
            exporter._clean_export_job()
        finally:
            exporter.persistence.delete()

    def test_from_file_configuration(self):
        try:
            test_manager = BasicExporter.from_file_configuration('./tests/data/basic_config.json')
            self.assertIsInstance(test_manager, BasicExporter)
            test_manager._clean_export_job()
        finally:
            test_manager.persistence.delete()
