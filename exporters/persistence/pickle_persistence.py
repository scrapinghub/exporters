import os
import re
import yaml
from exporters.persistence.base_persistence import BasePersistence
import pickle
import uuid


class PicklePersistence(BasePersistence):
    """
    Manages persistence using pickle module loading and dumping as a backend.

    Needed parameters:

        - file_path (str)
            Path to store the pickle file
    """
    parameters = {
        'file_path': {'type': basestring, 'required': False, 'default': '.'}
    }

    uri_regex = "pickle:(([a-zA-Z\d-]|\/)+)"

    def __init__(self, options, settings):
        super(PicklePersistence, self).__init__(options, settings)

    def get_last_position(self):
        if not os.path.isfile(os.path.join(self.read_option('file_path'), self.job_id)):
            raise ValueError('Trying to resume job {}, but path {} does not exist or is a directory.'
                             .format(self.job_id, os.path.join(self.read_option('file_path'), self.job_id)))

        persistence_file = open(os.path.join(self.read_option('file_path'), self.job_id), 'r')
        persistence_object = pickle.load(persistence_file)
        persistence_file.close()
        self.last_position = persistence_object['last_position']
        return self.last_position

    def commit_position(self, last_position=None):
        self.last_position = last_position
        persistence_object = {'job_id': self.job_id, 'last_position': self.last_position, 'configuration': str(self.configuration)}
        persistence_file = open(os.path.join(self.read_option('file_path'), self.job_id), 'w')
        pickle.dump(persistence_object, persistence_file)
        persistence_file.close()
        self.logger.debug('Commited batch number ' + str(self.last_position) + ' of job: ' + self.job_id)

    def generate_new_job(self):
        job_id = str(uuid.uuid4())
        persistence_object = {'job_id': job_id, 'last_position': None, 'configuration': str(self.configuration)}
        persistence_file = open(os.path.join(self.read_option('file_path'), job_id), 'w')
        pickle.dump(persistence_object, persistence_file)
        persistence_file.close()
        self.logger.debug('Created persistence pickle file in ' + self.read_option('file_path') + job_id)
        return job_id

    def delete_instance(self):
        os.remove(os.path.join(self.read_option('file_path'), self.job_id))

    @staticmethod
    def configuration_from_uri(uri, uri_regex):
        """
        returns a configuration object.
        """
        file_path = re.match(uri_regex, uri).groups()[0]
        with open(file_path) as f:
            configuration = pickle.load(f)['configuration']
        configuration = yaml.safe_load(configuration)
        configuration['exporter_options']['RESUME'] = True
        job_id = file_path.split(os.path.sep)[-1]
        configuration['exporter_options']['JOB_ID'] = job_id
        return configuration
