import unittest
from exporters.export_managers.unified_exporter import UnifiedExporter
from exporters.readers.random_reader import RandomReader
from exporters.transform.no_transform import NoTransform
from exporters.writers.console_writer import ConsoleWriter


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
                }
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
            'transform': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {

                }
            },
            'writer':{
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
        self.manager = UnifiedExporter(self.options)

    def test_parses_the_options_and_loads_pipeline_items(self):
        self.assertTrue(isinstance(self.manager.reader, RandomReader))
        self.assertTrue(isinstance(self.manager.writer, ConsoleWriter))
        self.assertTrue(isinstance(self.manager.transform, NoTransform))

    def test_from_file_configuration(self):
        test_manager = UnifiedExporter.from_file_configuration('./tests/data/basic_config.json')
        self.assertIsInstance(test_manager, UnifiedExporter)
