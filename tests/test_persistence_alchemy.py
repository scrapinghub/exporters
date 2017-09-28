from __future__ import absolute_import
import shutil
import tempfile
import unittest
import sqlite3
from copy import deepcopy

from exporters.exporter_config import ExporterConfig
from exporters.persistence.alchemy_persistence import (MysqlPersistence,
                                                       PostgresqlPersistence,
                                                       SqlitePersistence)

from .utils import meta, valid_config_with_updates


def query_db(dbfile, query):
    conn = sqlite3.connect(dbfile)
    conn.row_factory = sqlite3.Row
    return [dict(d) for d in conn.execute(query)]


class SqlitePersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp_folder = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_folder)

    def test_parse_persistence_uri(self):
        uri = 'sqlite://some/file.db:1234'
        self.assertEqual(('sqlite+pysqlite:///some/file.db', 1234),
                         SqlitePersistence.parse_persistence_uri(uri))

    def test_create_commit_and_get_position(self):
        # given:
        dbfile = '%s/dbfile.db' % self.tmp_folder
        options = {'database': dbfile}

        # when:
        persistence = SqlitePersistence(dict(options=options), meta())
        persistence.commit_position(dict(read=10000))
        persistence.commit_position(dict(read=20000))

        # then:
        result = query_db(dbfile, 'SELECT * FROM job WHERE id = %s'
                          % persistence.persistence_state_id)
        self.assertEqual(dict(read=20000), persistence.get_last_position())
        self.assertFalse(result[0]['job_finished'], "Job should not marked as finished")

    def test_close_should_mark_as_finished(self):
        # given:
        dbfile = '%s/dbfile.db' % self.tmp_folder
        options = {'database': dbfile}

        # when:
        persistence = SqlitePersistence(dict(options=options), meta())
        persistence.commit_position(dict(read=10000))
        persistence.close()

        # then:
        result = query_db(dbfile, 'SELECT * FROM job')
        self.assertTrue(result[0]['job_finished'], "Job should be marked as finished")

    def test_generate_new_job(self):
        # given:
        dbfile = '%s/dbfile.db' % self.tmp_folder
        persistence = SqlitePersistence(dict(options={'database': dbfile}), meta())

        # when:
        db_id = persistence.generate_new_job()

        # then:
        self.assertEqual(len(query_db(dbfile, 'SELECT 1 FROM job')), 2)
        self.assertEqual(len(query_db(dbfile, 'SELECT 1 FROM job WHERE id = %s' % db_id)), 1)

    def test_configuration_from_uri(self):
        self.maxDiff = None
        # given:
        dbfile = '%s/dbfile.db' % self.tmp_folder
        configuration = valid_config_with_updates({
            'persistence': {
                'name': 'exporters.persistence.alchemy_persistence.SqlitePersistence',
                'options': {
                    'database': dbfile,
                }
            },
            'exporter_options': {'prevent_bypass': True, 'resume': False},
        })
        config = ExporterConfig(configuration)
        persistence = SqlitePersistence(config.persistence_options, meta())
        job_id = persistence.persistence_state_id

        # when:
        persistence_uri = 'sqlite://%s:%s' % (dbfile, job_id)
        recovered_config = SqlitePersistence.configuration_from_uri(persistence_uri)

        # then:
        expected = deepcopy(configuration)
        expected['exporter_options']['resume'] = True
        expected['exporter_options']['persistence_state_id'] = job_id

        expected_exporter_options = dict(configuration['exporter_options'],
                                         resume=True, persistence_state_id=job_id)
        self.assertEqual(expected_exporter_options, recovered_config['exporter_options'])
        self.assertEqual(expected['reader'], recovered_config['reader'])
        self.assertEqual(expected['writer'], recovered_config['writer'])

        # TODO: figure out why the following assertion doesn't work
        # self.assertEqual(expected['persistence'], recovered_config['persistence'])


class MysqlPersistenceTest(unittest.TestCase):
    def test_parse_persistence_uri(self):
        uri = 'mysql://user:pass@host:3306/dbname/1234'
        self.assertEqual(('mysql://user:pass@host:3306/dbname', 1234),
                         MysqlPersistence.parse_persistence_uri(uri))


class PostgresqlPersistenceTest(unittest.TestCase):
    def test_parse_persistence_uri(self):
        uri = 'postgresql://user:pass@host:3306/dbname/1234'
        self.assertEqual(('postgresql://user:pass@host:3306/dbname', 1234),
                         PostgresqlPersistence.parse_persistence_uri(uri))
