import unittest
from mock import patch
from exporters.export_managers.settings import Settings
from exporters.exporter_options import ExporterOptions
from exporters.persistence.alchemy_persistence import MysqlPersistence, PostgresqlPersistence
from exporters.persistence.base_persistence import BasePersistence
from exporters.persistence.pickle_persistence import PicklePersistence


class BasePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter':  {}
            },
            'reader': {},
            'persistence': {},
            'writer': {}
        }
        self.settings = Settings(self.options['exporter_options'])

    def test_get_last_position(self):
        exporter_options = ExporterOptions(self.options)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_options, self.settings)
            persistence.get_last_position()

    def test_commit_position(self):
        exporter_options = ExporterOptions(self.options)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_options, self.settings)
            persistence.commit_position(1)

    def test_generate_new_job(self):
        exporter_options = ExporterOptions(self.options)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_options, self.settings)
            persistence.generate_new_job()

    def test_delete_instance(self):
        exporter_options = ExporterOptions(self.options)
        with self.assertRaises(NotImplementedError):
            persistence = BasePersistence(exporter_options, self.settings)
            persistence.delete_instance()


class PicklePersistenceTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': False,
                'formatter':  {}
            },
            'reader': {},
            'writer': {}
        }
        self.settings = Settings(self.options['exporter_options'])

    @patch('pickle.dump')
    @patch('uuid.uuid4')
    def test_create_persistence_job(self, mock_uuid, mock_pickle):
        mock_pickle.dump.return_value = True
        mock_uuid.return_value = 1
        exporter_options = ExporterOptions(self.options)
        persistence = PicklePersistence(exporter_options, self.settings)
        self.assertIsInstance(persistence, PicklePersistence)
        persistence.delete_instance()

    @patch('os.path.isfile', autospec=True)
    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('pickle.load', autospec=True)
    def test_get_last_position(self, mock_load_pickle, mock_dump_pickle, mock_open, mock_is_file):
        mock_dump_pickle.return_value = True
        mock_is_file.return_value = True
        mock_load_pickle.return_value = {'last_position': 10}
        exporter_options = ExporterOptions(self.options)
        persistence = PicklePersistence(exporter_options, self.settings)
        self.assertEqual(10, persistence.get_last_position())


    @patch('__builtin__.open', autospec=True)
    @patch('pickle.dump', autospec=True)
    @patch('uuid.uuid4', autospec=True)
    def test_commit(self, mock_uuid, mock_dump_pickle, mock_open):
        mock_dump_pickle.return_value = True
        mock_uuid.return_value = 1
        exporter_options = ExporterOptions(self.options)
        persistence = PicklePersistence(exporter_options, self.settings)
        self.assertEqual(None, persistence.commit_position(10))



class MysqlPersistenceTest(unittest.TestCase):

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.Session.query')
    def test_create_persistence_job_resume(self, mock_query, mock_metadata):
        options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': True,
                'JOB_ID': '',
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_query.return_value.filter.return_value.first.return_value = {}
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        self.assertIsInstance(persistence, MysqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_commit(self, mock_commit, mock_metadata, mock_query):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        persistence.commit_position(10)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_delete(self, mock_commit, mock_metadata, mock_query):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        persistence.delete_instance()

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_get_last_position(self,  mock_commit, mock_metadata, mock_query):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = MysqlPersistence(exporter_options, settings)
        self.assertTrue(persistence.get_last_position() == 0)


class PostgresqlPersistenceTest(unittest.TestCase):

    def setUp(self):
        self.options = {
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
            },
            'reader': {},
            'writer': {}
        }
        self.settings = Settings(self.options['exporter_options'])

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_create_persistence_job(self, mock_commit, mock_metadata):
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_options = ExporterOptions(self.options)
        persistence = PostgresqlPersistence(exporter_options, self.settings)
        self.assertIsInstance(persistence, PostgresqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_get_last_position(self,  mock_commit, mock_metadata, mock_query):
        mock_metadata.return_value = True
        mock_commit.return_value = True
        exporter_options = ExporterOptions(self.options)
        persistence = PostgresqlPersistence(exporter_options, self.settings)
        self.assertTrue(persistence.get_last_position() == 0)

    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.Session.query')
    def test_create_persistence_job_resume(self, mock_query, mock_metadata):
        options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'resume': True,
                'JOB_ID': '',
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_query.return_value.filter.return_value.first.return_value = {}
        exporter_options = ExporterOptions(options)
        persistence = PostgresqlPersistence(exporter_options, settings)
        self.assertIsInstance(persistence, PostgresqlPersistence)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_commit(self, mock_commit, mock_metadata, mock_query):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = PostgresqlPersistence(exporter_options, settings)
        persistence.commit_position(10)

    @patch('sqlalchemy.orm.session.Session.query')
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('sqlalchemy.orm.session.SessionTransaction.commit')
    def test_delete(self, mock_commit, mock_metadata, mock_query):
        options = {
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
            },
            'reader': {},
            'writer': {}
        }
        settings = Settings(options['exporter_options'])
        mock_metadata.return_value = True
        mock_commit.return_value = {}
        mock_query.return_value.filter.return_value.update.return_value = True
        exporter_options = ExporterOptions(options)
        persistence = PostgresqlPersistence(exporter_options, settings)
        persistence.delete_instance()