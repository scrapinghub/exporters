from exporters.logger.base_logger import ReaderLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseReader(BasePipelineItem):
    """
    This module reads and creates a batch to pass them to the pipeline. It can implement the following methods:
    """

    def __init__(self, configuration):
        super(BaseReader, self).__init__(configuration)
        self.finished = False
        self.logger = ReaderLogger(configuration.get('settings', {}))
        self.last_position = 0

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list of BaseRecord objects. When it has nothing else to read,
        it must set class variable "finished" to True.
        """
        raise NotImplementedError

    def is_finished(self):
        """
        Returns whether if there are items left to be read or not.
        """
        return self.finished

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited by the writer, in order to
        have resume support
        """
        self.last_position = last_position

    def get_last_position(self):
        """
        Returns the last read position.
        """
        return self.last_position
