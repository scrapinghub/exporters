import unittest
from mock import patch, Mock
from exporters.exporter_config import ExporterConfig
from exporters.persistence.alchemy_persistence import MysqlPersistence, PostgresqlPersistence
from exporters.persistence.base_persistence import BasePersistence
from exporters.persistence.pickle_persistence import PicklePersistence
from exporters.utils import remove_if_exists
from .utils import valid_config_with_updates


class BasePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.config = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter':  {}
            }
        })

    def test_get_last_position(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options)
            persistence.get_last_position()

    def test_commit_position(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options)
            persistence.commit_position(1)

    def test_generate_new_job(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options)
            persistence.generate_new_job()

    def test_delete_instance(self):
        exporter_config = ExporterConfig(self.config)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_config.persistence_options)
            persistence.close()


class PicklePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.config = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter':  {}
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
            persistence = PicklePersistence(exporter_config.persistence_options)
            self.assertIsInstance(persistence, PicklePersistence)
            persistence.close()
        finally:
            remove_if_exists(file_name)

    @patch('os.path.isfile', autospec=True)
    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('pickle.load', autospec=True)
    def test_get_last_position(self, mock_load_pickle, mock_dump_pickle, mock_open, mock_is_file):
        mock_dump_pickle.return_value = True
        mock_is_file.return_value = True
        mock_load_pickle.return_value = {'last_position': 10}
        exporter_config = ExporterConfig(self.config)
        persistence = PicklePersistence(exporter_config.persistence_options)
        self.assertEqual(10, persistence.get_last_position())

    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('uuid.uuid4', autospec=True)
    def test_commit(self, mock_uuid, mock_dump_pickle, mock_open):
        mock_dump_pickle.return_value = True
        mock_uuid.return_value = 1
        exporter_config = ExporterConfig(self.config)
        persistence = PicklePersistence(exporter_config.persistence_options)
        self.assertEqual(None, persistence.commit_position(10))
        self.assertEqual(persistence.stats['commited_positions'], 1)


class MysqlPersistenceTest(unittest.TestCase):

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.orm.session.Session.add')
    @patch('exporters.persistence.base_sqlalchemy_persistence.Job')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.Session.query')
    def test_create_persistence_job_resume(self, mock_query, mock_metadata, mock_job, mock_add):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': True,
                'persistence_state_id': '',
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_job = Mock(last_position='0')
        mock_query.return_value.filter.return_value.first.return_value = mock_job
        mock_add.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_commit(self, mock_commit, mock_metadata, mock_query):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        persistence.commit_position(10)
        self.assertEqual(persistence.stats['commited_positions'], 1)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_delete(self, mock_commit, mock_metadata, mock_query):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        persistence.close()

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_get_last_position(self,  mock_commit, mock_metadata, mock_query):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.MysqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = True
        mock_query.return_value.filter.return_value.first.return_value.last_position = '0'
        exporter_config = ExporterConfig(options)
        persistence = MysqlPersistence(exporter_config.persistence_options)
        self.assertTrue(persistence.get_last_position() == 0)


class PostgresqlPersistenceTest(unittest.TestCase):

    def setUp(self):
        self.config = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.PostgresqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_config = ExporterConfig(self.config)
        persistence = PostgresqlPersistence(exporter_config.persistence_options)
        self.assertIsInstance(persistence, PostgresqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_get_last_position(self,  mock_commit, mock_metadata, mock_query):
        mock_metadata.return_value = True
        mock_commit.return_value = True
        mock_query.return_value.filter.return_value.first.return_value.last_position = '0'
        exporter_config = ExporterConfig(self.config)
        persistence = PostgresqlPersistence(exporter_config.persistence_options)
        self.assertTrue(persistence.get_last_position() == 0)

    @patch('sqlalchemy.orm.session.Session.add')
    @patch('exporters.persistence.base_sqlalchemy_persistence.Job')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.Session.query')
    def test_create_persistence_job_resume(self, mock_query, mock_metadata, mock_job, mock_add):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': True,
                'persistence_state_id': '',
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.PostgresqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_job = Mock(last_position='0')
        mock_query.return_value.filter.return_value.first.return_value = mock_job
        mock_add.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = PostgresqlPersistence(exporter_config.persistence_options)
        self.assertIsInstance(persistence, PostgresqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_commit(self, mock_commit, mock_metadata, mock_query):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.PostgresqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = PostgresqlPersistence(exporter_config.persistence_options)
        persistence.commit_position(10)
        self.assertEqual(persistence.stats['commited_positions'], 1)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_delete(self, mock_commit, mock_metadata, mock_query):
        options = valid_config_with_updates({
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter': {}
            },
            'persistence':{
                'name': 'exporters.persistence.alchemy_persistence.PostgresqlPersistence',
                'options': {
                    'user': 'user',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_persistence'
                }
            }
        })
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_config = ExporterConfig(options)
        persistence = PostgresqlPersistence(exporter_config.persistence_options)
        persistence.close()
