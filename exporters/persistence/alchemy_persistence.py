import datetime
import json
import re

import yaml
from sqlalchemy import Boolean, Column, DateTime, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from exporters.persistence.base_persistence import BasePersistence


Base = declarative_base()


class Job(Base):
    __tablename__ = 'job'
    id = Column(Integer, primary_key=True)
    last_position = Column(Text, nullable=False)
    last_committed = Column(DateTime)
    job_finished = Column(Boolean)
    configuration = Column(Text, nullable=False)


class BaseAlchemyPersistence(BasePersistence):
    supported_options = {
        'user': {'type': basestring},
        'password': {'type': basestring},
        'host': {'type': basestring},
        'port': {'type': int},
        'database': {'type': basestring}
    }
    PROTOCOL = None

    # example: mysql://username:password@host:port/database/JOB_ID
    persistence_uri_re = (r'(?P<proto>[a-z]+)://(?P<user>.+):(?P<password>.+)@'
                          r'(?P<host>.+):(?P<port>\d+)/(?P<database>.+)/(?P<job_id>\d+)')

    def __init__(self, *args, **kwargs):
        self.engine = None
        super(BaseAlchemyPersistence, self).__init__(*args, **kwargs)

    def _db_init(self):
        db_uri = self.build_db_conn_uri(
            proto=self.PROTOCOL,
            user=self.read_option('user'),
            password=self.read_option('password'),
            host=self.read_option('host'),
            port=self.read_option('port'),
            database=self.read_option('database'),
        )
        self.engine = create_engine(db_uri)
        Base.metadata.create_all(self.engine)
        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()

    def get_last_position(self):
        if not self.engine:
            self._db_init()
        job = self.session.query(Job).filter(Job.id == self.persistence_state_id).first()
        return json.loads(job.last_position)

    def commit_position(self, last_position=None):
        self.last_position = last_position

        self.session.query(Job).filter(Job.id == self.persistence_state_id).update(
            {"last_position": json.dumps(self.last_position),
             "last_committed": datetime.datetime.now()}, synchronize_session='fetch')
        self.session.commit()
        self.logger.debug('Commited batch number ' + str(self.last_position) +
                          ' of job: ' + str(self.persistence_state_id))
        self.set_metadata('commited_positions',
                          self.get_metadata('commited_positions') + 1)

    def generate_new_job(self):
        if not self.engine:
            self._db_init()
        new_job = Job(last_position='None', configuration=json.dumps(self.configuration))
        self.session.add(new_job)
        self.session.commit()
        self.persistence_state_id = new_job.id
        self.logger.debug(
            'Created persistence job with id {} in database {}. Using protocol {}.{}'.format(
                new_job.id, self.read_option('database'), self.PROTOCOL, str(new_job.id)))
        return new_job.id

    def close(self):
        self.session.query(Job).filter(Job.id == self.persistence_state_id).update(
            dict(job_finished=True, last_committed=datetime.datetime.now()),
            synchronize_session='fetch'
        )
        self.session.commit()
        self.session.close()

    @classmethod
    def build_db_conn_uri(cls, **kwargs):
        """Build the database connection URI from the given keyword arguments
        """
        return '{proto}://{user}:{password}@{host}:{port}/{database}'.format(**kwargs)

    @classmethod
    def parse_persistence_uri(cls, persistence_uri):
        """Parse a database URI and the persistence state ID from
        the given persistence URI
        """
        regex = cls.persistence_uri_re
        match = re.match(regex, persistence_uri)
        if not match:
            raise ValueError("Couldn't parse persistence URI: %s -- regex: %s)"
                             % (persistence_uri, regex))

        conn_params = match.groupdict()
        missing = {'proto', 'job_id', 'database'} - set(conn_params)
        if missing:
            raise ValueError('Missing required parameters: %s (given params: %s)'
                             % (tuple(missing), conn_params))

        persistence_state_id = int(conn_params.pop('job_id'))
        db_uri = cls.build_db_conn_uri(**conn_params)
        return db_uri, persistence_state_id

    @classmethod
    def configuration_from_uri(cls, persistence_uri):
        """
        Return a configuration object.
        """
        db_uri, persistence_state_id = cls.parse_persistence_uri(persistence_uri)
        engine = create_engine(db_uri)
        Base.metadata.create_all(engine)
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        job = session.query(Job).filter(Job.id == persistence_state_id).first()
        configuration = job.configuration
        configuration = yaml.safe_load(configuration)
        configuration['exporter_options']['resume'] = True
        configuration['exporter_options']['persistence_state_id'] = persistence_state_id
        return configuration


_docstring = """
Manage export persistence using a {protocol} database as a backend.
It will add a row for every job in a table called Jobs.

- user (str)
Username with access to {protocol} database

- password (str)
Password string

- host (str)
DB server host ip

- port (int)
DB server port

- database (str)
Name of the database in which store jobs persistence
"""


class MysqlPersistence(BaseAlchemyPersistence):
    PROTOCOL = 'mysql'
    __doc__ = _docstring.format(protocol=PROTOCOL)


class PostgresqlPersistence(BaseAlchemyPersistence):
    PROTOCOL = 'postgresql'
    __doc__ = _docstring.format(protocol=PROTOCOL)


class SqlitePersistence(BaseAlchemyPersistence):
    PROTOCOL = 'postgresql'
    __doc__ = _docstring.format(protocol=PROTOCOL)
    # sqlite://path/to/file.db:JOB_ID
    persistence_uri_re = '(?P<proto>sqlite)://(?P<database>.+):(?P<job_id>\d+)'
    supported_options = {
        # set defaults for unneeded options
        'user': {'type': basestring, 'default': ''},
        'password': {'type': basestring, 'default': ''},
        'host': {'type': basestring, 'default': ''},
        'port': {'type': int, 'default': ''},
    }

    @classmethod
    def build_db_conn_uri(self, **kwargs):
        return 'sqlite+pysqlite:///%s' % kwargs.pop('database')
