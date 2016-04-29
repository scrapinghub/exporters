import unittest
from mock import patch
from exporters.exporter_config import ExporterConfig
from exporters.persistence.base_persistence import BasePersistence
from exporters.persistence.pickle_persistence import PicklePersistence
from exporters.utils import remove_if_exists

from .utils import valid_config_with_updates, meta


class BasePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.config = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
            }
        })

    def test_get_last_position(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options, meta())
            persistence.get_last_position()

    def test_commit_position(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options, meta())
            persistence.commit_position(1)

    def test_generate_new_job(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options, meta())
            persistence.generate_new_job()

    def test_delete_instance(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options, meta())
            persistence.close()


class PicklePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.config = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
            },
            'persistence': {
                'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                'options': {'file_path': '/tmp'}
            }
        })

    @patch('pickle.dump')
    @patch('uuid.uuid4')
    def test_create_persistence_job(self, mock_uuid, mock_pickle):
        file_name = '1'
        mock_pickle.dump.return_value = True
        mock_uuid.return_value = file_name
        exporter_config = ExporterConfig(self.config)
        try:
            persistence = PicklePersistence(
                exporter_config.persistence_options, meta())
            self.assertIsInstance(persistence, PicklePersistence)
            persistence.close()
        finally:
            remove_if_exists('/tmp/'+file_name)

    @patch('os.path.isfile', autospec=True)
    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('pickle.load', autospec=True)
    def test_get_last_position(self, mock_load_pickle, mock_dump_pickle, mock_open, mock_is_file):
        mock_dump_pickle.return_value = True
        mock_is_file.return_value = True
        mock_load_pickle.return_value = {'last_position': {'last_key': 10}}
        exporter_config = ExporterConfig(self.config)
        persistence = PicklePersistence(exporter_config.persistence_options, meta())
        self.assertEqual({'last_key': 10}, persistence.get_last_position())

    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('uuid.uuid4', autospec=True)
    def test_commit(self, mock_uuid, mock_dump_pickle, mock_open):
        mock_dump_pickle.return_value = True
        mock_uuid.return_value = 1
        exporter_config = ExporterConfig(self.config)
        persistence = PicklePersistence(exporter_config.persistence_options, meta())
        self.assertEqual(None, persistence.commit_position(10))
        self.assertEqual(persistence.get_metadata('commited_positions'), 1)
