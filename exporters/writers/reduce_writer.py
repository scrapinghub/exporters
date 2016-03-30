import six
from .base_writer import BaseWriter
from exporters.exceptions import ConfigurationError


def compile_reduce_function(reduce_code, source_path=None):
    # XXX: potential security hole -- only use this in contained environments
    ns = {}
    exec(compile(reduce_code, source_path or '<string>', 'exec'), {}, ns)
    try:
        return ns['reduce_function']
    except KeyError:
        raise ConfigurationError(
            "Missing definition of reduce_function(item, accumulator=None)")


class ReduceWriter(BaseWriter):
    """
    This writer allow exporters to make aggregation of items data and print the results

        - code (str)
            Python code defining a reduce_function(item, accumulator=None)
    """

    supported_options = {
        "code": {
            'type': six.string_types,
            'help': "Python code defining a reduce_function(item, accumulator=None)"
        },
        "source_path": {
            'type': six.string_types,
            'default': None,
            'help': 'Source path, useful for debugging/inspecting tools',
        }
    }

    def __init__(self, *args, **kwargs):
        super(ReduceWriter, self).__init__(*args, **kwargs)
        code = self.read_option('code')
        self.logger.warning(
            'ReduceWriter uses Python exec() -- only use it in contained environments')
        source_path = self.read_option('source_path')
        self.reduce_function = compile_reduce_function(code, source_path)
        self.logger.info('ReduceWriter configured with code:\n%s\n' % code)
        self._accumulator = None

    def write_batch(self, batch):
        for item in batch:
            self._accumulator = self.reduce_function(item, self._accumulator)
            self.increment_written_items()
        self.logger.info('Reduced {} items, accumulator is: {}'.format(
            self.get_metadata('items_count'), self._accumulator))

    @property
    def reduced_result(self):
        return self._accumulator
