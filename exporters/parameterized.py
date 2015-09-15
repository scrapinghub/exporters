"""
Implements parameters and options base code
"""

class Parameterized(object):

    base_parameters = {}
    parameters = {}
    
    def __new__(cls, options, settings):
        obj = object.__new__(cls)
        object.__init__(obj, options, settings)
        obj.parameters = cls.base_parameters.copy()
        obj.parameters.update(cls.parameters)
        return obj

    def __init__(self, options, settings):
        self.settings = settings
        self.options = options.get('options', {})
        self.configuration = None
        self.check_options()

    def set_configuration(self, configuration):
        self.configuration = configuration

    def check_options(self):
        for parameter_name, parameter_info in self.parameters.iteritems():
            parameter_value = self.read_option(parameter_name)
            if parameter_value and not isinstance(parameter_value, parameter_info['type']):
                raise ValueError('Parameter ' + parameter_name + ' should be type: ' + str(parameter_info['type']))
            if 'default' in parameter_info:
                continue
            if not parameter_value:
                raise ValueError('Options object should have parameter: ' + parameter_name)

    def read_option(self, option_name, default=None):
        if option_name in self.options:
            return self.options.get(option_name)
        return self.parameters.get(option_name, {}).get('default', default)
