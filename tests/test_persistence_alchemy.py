import unittest
from mock import patch, Mock
from exporters.persistence.alchemy_persistence import MysqlPersistence, PostgresqlPersistence
from .utils import valid_config_with_updates, meta
from exporters.exporter_config import ExporterConfig


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
            'persistence': {
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
        persistence = MysqlPersistence(exporter_config.persistence_options, meta())
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.orm.session.Session.add')
    @patch('exporters.persistence.alchemy_persistence.Job')
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
            'persistence': {
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
        persistence = MysqlPersistence(exporter_config.persistence_options, meta())
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
            'persistence': {
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
        persistence = MysqlPersistence(exporter_config.persistence_options, meta())
        persistence.commit_position(10)
        self.assertEqual(persistence.get_metadata('commited_positions'), 1)

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
            'persistence': {
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
        persistence = MysqlPersistence(exporter_config.persistence_options, meta())
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
            'persistence': {
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
        persistence = MysqlPersistence(exporter_config.persistence_options, meta())
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
            'persistence': {
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
        persistence = PostgresqlPersistence(exporter_config.persistence_options, meta())
        self.assertIsInstance(persistence, PostgresqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_get_last_position(self,  mock_commit, mock_metadata, mock_query):
        mock_metadata.return_value = True
        mock_commit.return_value = True
        mock_query.return_value.filter.return_value.first.return_value.last_position = '0'
        exporter_config = ExporterConfig(self.config)
        persistence = PostgresqlPersistence(exporter_config.persistence_options, meta())
        self.assertTrue(persistence.get_last_position() == 0)

    @patch('sqlalchemy.orm.session.Session.add')
    @patch('exporters.persistence.alchemy_persistence.Job')
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
            'persistence': {
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
        persistence = PostgresqlPersistence(exporter_config.persistence_options, meta())
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
            'persistence': {
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
        persistence = PostgresqlPersistence(exporter_config.persistence_options, meta())
        persistence.commit_position(10)
        self.assertEqual(persistence.get_metadata('commited_positions'), 1)

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
            'persistence': {
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
        persistence = PostgresqlPersistence(exporter_config.persistence_options, meta())
        persistence.close()

