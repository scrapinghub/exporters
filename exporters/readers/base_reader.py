from exporters.logger.base_logger import ReaderLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseReader(BasePipelineItem):
    """
    This module reads and creates a batch to pass them to the pipeline
    """

    def __init__(self, options, metadata):
        super(BaseReader, self).__init__(options, metadata)
        self.finished = False
        self.logger = ReaderLogger({
            'log_level': options.get('log_level'),
            'logger_name': options.get('logger_name')
        })
        self.last_position = {}
        self.set_metadata('read_items', 0)

    def increase_read(self):
        self.set_metadata('read_items',
                          self.get_metadata('read_items') + 1)

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        raise NotImplementedError

    def is_finished(self):
        """
        Returns whether if there are items left to be read or not.
        """
        return self.finished

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        self.last_position = last_position

    def get_last_position(self):
        """
        Returns the last read position.
        """
        return self.last_position

    def set_metadata(self, key, value, module='reader'):
        super(BaseReader, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='reader'):
        super(BaseReader, self).update_metadata(data, module)

    def get_metadata(self, key, module='reader'):
        return super(BaseReader, self).get_metadata(key, module)

    def get_all_metadata(self, module='reader'):
        return super(BaseReader, self).get_all_metadata(module)

    def close(self):
        pass
