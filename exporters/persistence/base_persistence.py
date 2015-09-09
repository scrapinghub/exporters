from exporters.logger.base_logger import PersistenceLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BasePersistence(BasePipelineItem):
    """
    This module is in charge of resume support. It must be able to persist the current state of read and written items, and
    inform of that state on demand. It can implement the following methods:
    """

    def __init__(self, options, settings):
        super(BasePersistence, self).__init__(options.persistence_options, settings)
        self.requirements = getattr(self, 'requirements', {})
        self.settings = settings
        self.configuration = options
        self.check_options()
        self.logger = PersistenceLogger(self.settings)
        if not settings.get('RESUME'):
            self.job_id = self.generate_new_job()
            self.logger.info('Created job with id: ' + str(self.job_id))
            self.last_position = None
        else:
            self.job_id = settings.get('JOB_ID')
            self.last_position = self.get_last_position()
            self.logger.info('Resumed job with id: ' + str(self.job_id))

    def get_last_position(self):
        """
        Returns the last commited position.
        """
        raise NotImplementedError

    def commit_position(self, last_position):
        """
        Commits a position that has been through all the pipeline. Position can be any serializable object. This support both
        usual position abstractions (number of batch) of specific abstractions such as offsets in Kafka (which are a dict).
        """
        raise NotImplementedError

    def generate_new_job(self):
        """
        Creates and instantiates all that is needed to keep persistence (tmp files, remote connections...).
        """
        raise NotImplementedError

    def delete_instance(self):
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