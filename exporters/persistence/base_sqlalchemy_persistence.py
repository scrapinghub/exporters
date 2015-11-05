import datetime
import json
import re
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import yaml
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

    def __init__(self, options):
        self.engine = None
        super(BaseAlchemyPersistence, self).__init__(options)

    def _db_init(self):
        user = self.read_option('user')
        password = self.read_option('password')
        host = self.read_option('host')
        port = self.read_option('port')
        database = self.read_option('database')
        self.engine = create_engine(
            '{}://{}:{}@{}:{}/{}'.format(self.PROTOCOL, user, password, host, port, database))
        Base.metadata.create_all(self.engine)
        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()

    def get_last_position(self):
        if not self.engine:
            self._db_init()
        job = self.session.query(Job).filter(Job.id == self.persistence_state_id).first()
        last_position = json.loads(job.last_position)
        return last_position

    def commit_position(self, last_position=None):
        self.last_position = last_position

        self.session.query(Job).filter(Job.id == self.persistence_state_id).update(
            {"last_position": json.dumps(self.last_position),
             "last_committed": datetime.datetime.now()}, synchronize_session='fetch')
        self.session.commit()
        self.logger.debug('Commited batch number ' + str(self.last_position) + ' of job: ' + str(self.persistence_state_id))
        self.stats['commited_positions'] += 1

    def generate_new_job(self):
        if not self.engine:
            self._db_init()
        new_job = Job(last_position='None', configuration=str(self.configuration))
        self.session.add(new_job)
        self.session.commit()
        self.persistence_state_id = new_job.id
        self.logger.debug('Created persistence job with id {} in database {}. Using protocol {}.'
                          .format(new_job.id, self.read_option('database'), self.PROTOCOL) + str(new_job.id))
        return new_job.id

    def delete_instance(self):
        self.session.query(Job).filter(Job.id == self.persistence_state_id).update(
            {"job_finished": True, "last_committed": datetime.datetime.now()}, synchronize_session='fetch')
        self.session.commit()
        self.session.close()


    @staticmethod
    def configuration_from_uri(uri, uri_regex):
        """
        returns a configuration object.
        """
        connection_parameters = re.match(uri_regex, uri).groups()
        user, password, host, port, database, persistence_state_id = connection_parameters
        engine = create_engine(
            '{}://{}:{}@{}:{}/{}'.format(uri.split('://')[0], user, password, host, port, database))
        Base.metadata.create_all(engine)
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        job = session.query(Job).filter(Job.id == int(persistence_state_id)).first()
        configuration = job.configuration
        configuration = yaml.safe_load(configuration)
        configuration['exporter_options']['resume'] = True
        configuration['exporter_options']['persistence_state_id'] = persistence_state_id
        return configuration
