import logging


LEVEL_DICT = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


class BaseLogger(object):
    def __init__(self, options):
        level = LEVEL_DICT.get(options.get('log_level'), 'INFO')
        self.logger = logging.getLogger(options.get('logger_name', 'export-pipeline'))
        self.logger.setLevel(level)


class CategoryLogger(BaseLogger):

    def debug(self, msg):
        self._log(msg=msg, level=logging.DEBUG)

    def info(self, msg):
        self._log(msg=msg, level=logging.INFO)

    def warning(self, msg):
        self._log(msg=msg, level=logging.WARNING)

    def error(self, msg):
        self._log(msg=msg, level=logging.ERROR)

    def critical(self, msg):
        self._log(msg=msg, level=logging.CRITICAL)

    def _log(self, msg, level):
        self.logger.log(msg=msg, level=level)


class ReaderLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- READER -- '+msg, level=level)


class TransformLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- TRANSFORM -- '+msg, level=level)


class FilterLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- FILTER -- '+msg, level=level)


class WriterLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- WRITER -- '+msg, level=level)


class ExportManagerLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- EXPORTMANAGER -- '+msg, level=level)


class PersistenceLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- PERSISTENCE -- '+msg, level=level)


class StatsManagerLogger(CategoryLogger):

    def _log(self, msg, level):
        self.logger.log(msg=' -- STATS MANAGER -- '+msg, level=level)
