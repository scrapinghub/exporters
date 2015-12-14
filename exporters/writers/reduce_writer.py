from .base_writer import BaseWriter
from exporters.exceptions import ConfigurationError


def compile_reduce_function(reduce_code):
    # XXX: potential security hole -- only use this in contained environments
    exec(reduce_code)
    try:
        return locals()['reduce_function']
    except KeyError:
        raise ConfigurationError(
            "Missing definition of reduce_function(item, accumulator=None)")


class ReduceWriter(BaseWriter):
    supported_options = {
        "code": {
            'type': basestring,
            'help': "Python code defining a reduce_function(item, accumulator=None)"
        }
    }

    def __init__(self, *args, **kwargs):
        super(ReduceWriter, self).__init__(*args, **kwargs)
        code = self.read_option('code')
        self.logger.warning('ReduceWriter uses Python exec() -- only use it in contained environments')
        self.reduce_function = compile_reduce_function(code)
        self.logger.info('ReduceWriter configured with code:\n%s\n' % code)
        self._accumulator = None

    def write_batch(self, batch):
        for item in batch:
            self._accumulator = self.reduce_function(item, self._accumulator)
            self.increment_written_items()
        self.logger.info('Reduced {} items, accumulator is: {}'.format(self.items_count,
                                                                       self._accumulator))

    @property
    def reduced_result(self):
        return self._accumulator
