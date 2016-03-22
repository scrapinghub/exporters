import os
import pickle
import mock
import unittest
from exporters.export_managers.base_exporter import BaseExporter
from exporters.export_managers.basic_exporter import BasicExporter
from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.readers.random_reader import RandomReader
from exporters.transform.no_transform import NoTransform
from exporters.utils import TmpFile
from exporters.writers.console_writer import ConsoleWriter
from .utils import valid_config_with_updates


def get_filename(path, persistence_id):
    return os.path.join(path, persistence_id)


def fail(*a, **kw):
    raise ValueError("fail")


class FakeBypass(BaseBypass):
    bypass_called = False
    fake_meet_conditions = True

    def __init__(self, options, metadata=None):
        super(FakeBypass, self).__init__(options, metadata)

    def meets_conditions(self):
        if not self.fake_meet_conditions:
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
        self.assertEquals(10, exporter.writer.get_metadata('items_count'))

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
        expected_count = 10
        self.assertEquals(expected_count, exporter.writer.get_metadata('items_count'))

    def test_bypass_should_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())
        bypass_instance = FakeBypass(exporter.config)
        exporter.bypass_cases = [bypass_instance]

        # when:
        exporter.export()

        # then:
        self.assertTrue(bypass_instance.bypass_called, "Bypass should have been called")

    def test_when_unmet_conditions_bypass_should_not_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())
        bypass_instance = FakeBypass(exporter.config)
        bypass_instance.fake_meet_conditions = False
        exporter.bypass_cases = [bypass_instance]

        # when:
        exporter.export()

        # then:
        self.assertFalse(bypass_instance.bypass_called, "Bypass should NOT have been called")

    def test_when_meet_conditions_but_config_prevent_bypass_it_should_not_be_called(self):
        # given:
        config = self.build_config()
        config['exporter_options']['prevent_bypass'] = True
        self.exporter = exporter = BaseExporter(config)
        bypass_instance = FakeBypass(exporter.config)
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

    def test_resume_items(self):
        with TmpFile() as pickle_file:
            # given:
            persistence_data = {
                'last_position': {
                    'accurate_items_count': False,
                    'writer_metadata': {'items_count': 30},
                    'last_key': 3
                },
            }
            pickle.dump(persistence_data, open(pickle_file, 'w'))

            config = self.build_config(
                exporter_options={
                    'resume': True,
                    'persistence_state_id': os.path.basename(pickle_file),
                },
                persistence={
                    'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                    'options': {
                        'file_path': os.path.dirname(pickle_file)
                    }
                }
            )

            # when:
            exporter = BaseExporter(config)
            exporter._init_export_job()

            # then:
            self.assertEqual(30, exporter.writer.get_metadata('items_count'))
            self.assertFalse(exporter.metadata.accurate_items_count,
                             "Couldn't get accurate count from last_position")


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
        test_manager = BasicExporter.from_file_configuration('./tests/data/basic_config.json')
        try:
            self.assertIsInstance(test_manager, BasicExporter)
            test_manager._clean_export_job()
        finally:
            test_manager.persistence.delete()
