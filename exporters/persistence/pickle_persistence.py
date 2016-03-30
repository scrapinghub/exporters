import os
import re
import yaml
from exporters.persistence.base_persistence import BasePersistence
import pickle
import uuid
from exporters.utils import remove_if_exists


class PicklePersistence(BasePersistence):
    """
    Manages persistence using pickle module loading and dumping as a backend.

        - file_path (str)
            Path to store the pickle file
    """
    supported_options = {
        'file_path': {'type': basestring, 'default': '.'}
    }

    uri_regex = "pickle:(([a-zA-Z\d-]|\/)+)"

    def __init__(self, *args, **kwargs):
        super(PicklePersistence, self).__init__(*args, **kwargs)
        self.persistence_file_name = self._get_persistence_file_name()

    def _get_persistence_file_name(self):
        return os.path.join(self.read_option('file_path'), self.persistence_state_id)

    def get_last_position(self):
        if not os.path.isfile(self._get_persistence_file_name()):
            raise ValueError(
                'Trying to resume job {}, but path {}'
                'does not exist or is a directory.'.format(
                    self.persistence_state_id, self._get_persistence_file_name())
            )

        persistence_file = open(self._get_persistence_file_name(), 'r')
        persistence_object = pickle.load(persistence_file)
        persistence_file.close()
        self.last_position = persistence_object['last_position']
        return self.last_position

    def commit_position(self, last_position=None):
        self.last_position = last_position
        persistence_object = {
            'persistence_state_id': self.persistence_state_id,
            'last_position': self.last_position,
            'configuration': str(self.configuration)
        }
        with open(self._get_persistence_file_name(), 'w') as persistence_file:
            pickle.dump(persistence_object, persistence_file)
        self.logger.debug('Commited batch number ' + str(self.last_position) + ' of job: ' +
                          self.persistence_state_id)
        self.set_metadata('commited_positions',
                          self.get_metadata('commited_positions') + 1)

    def generate_new_job(self):
        self.persistence_state_id = str(uuid.uuid4())
        persistence_object = {
            'persistence_state_id': self.persistence_state_id,
            'last_position': None,
            'configuration': str(self.configuration)
        }
        with open(self._get_persistence_file_name(), 'w') as persistence_file:
            pickle.dump(persistence_object, persistence_file)

        self.logger.debug('Created persistence pickle file in ' +
                          self.read_option('file_path') + self.persistence_state_id)
        return self.persistence_state_id

    def close(self):
        pass

    @staticmethod
    def configuration_from_uri(uri, uri_regex):
        """
        returns a configuration object.
        """
        file_path = re.match(uri_regex, uri).groups()[0]
        with open(file_path) as f:
            configuration = pickle.load(f)['configuration']
        configuration = yaml.safe_load(configuration)
        configuration['exporter_options']['resume'] = True
        persistence_state_id = file_path.split(os.path.sep)[-1]
        configuration['exporter_options']['persistence_state_id'] = persistence_state_id
        return configuration

    def delete(self):
        remove_if_exists(self.persistence_file_name)
