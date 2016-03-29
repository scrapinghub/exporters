import json
from exporters.logger.base_logger import PersistenceLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BasePersistence(BasePipelineItem):
    """
    Base module for persistence modules
    """

    def __init__(self, options, metadata):
        super(BasePersistence, self).__init__(options, metadata)
        self.set_metadata('commited_positions', 0)
        self.configuration = json.loads(options.get('configuration', '{}'))
        self.logger = PersistenceLogger({
            'log_level': options.get('log_level'),
            'logger_name': options.get('logger_name')
        })
        self._load_persistence_options()
        self._start_persistence(options)

    def _start_new_job(self):
        self.persistence_state_id = self.generate_new_job()
        self.logger.info('Created job with id: ' + str(self.persistence_state_id))
        self.last_position = None

    def _resume_job(self, options):
        self.persistence_state_id = options.get('persistence_state_id')
        self.last_position = self.get_last_position()
        self.logger.info('Resumed job with id: ' + str(self.persistence_state_id))

    def _load_persistence_options(self):
        pass

    def _start_persistence(self, options):
        if not options.get('resume'):
            self._start_new_job()
        else:
            self._resume_job(options)

    def get_last_position(self):
        """
        Returns the last commited position.
        """
        raise NotImplementedError

    def commit_position(self, last_position):
        """
        Commits a position that has been through all the pipeline.
        Position can be any serializable object. This support both
        usual position abstractions (number of batch) of specific abstractions
        such as offsets in Kafka (which are a dict).
        """
        raise NotImplementedError

    def generate_new_job(self):
        """
        Creates and instantiates all that is needed to keep
        persistence (tmp files, remote connections...).
        """
        raise NotImplementedError

    def close(self):
        """
        Cleans tmp files, close remote connections...
        """
        raise NotImplementedError

    @staticmethod
    def configuration_from_uri(uri, regex):
        """
        returns a configuration object.
        """
        raise NotImplementedError

    def delete(self):
        pass

    def set_metadata(self, key, value, module='persistence'):
        super(BasePersistence, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='persistence'):
        super(BasePersistence, self).update_metadata(data, module)

    def get_metadata(self, key, module='persistence'):
        return super(BasePersistence, self).get_metadata(key, module)

    def get_all_metadata(self, module='persistence'):
        return super(BasePersistence, self).get_all_metadata(module)
