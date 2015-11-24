from .base_writer import BaseWriter


def compile_reduce_function(reduce_code):
    # XXX: ooops, huge security hole here!
    # DO NOT merge while this code is still here
    exec(reduce_code)
    return locals()['reduce_function']


class ReduceWriter(BaseWriter):
    supported_options = {
        "code": {
            'type': basestring,
            'help': "Python code, which should define a reduce_function"
        }
    }

    def __init__(self, *args, **kwargs):
        super(ReduceWriter, self).__init__(*args, **kwargs)
        code = self.read_option('code')
        self.reduce_function = compile_reduce_function(code)
