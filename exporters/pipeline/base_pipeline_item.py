from exporters.notifications.notifiers_list import NotifiersList


class BasePipelineItem(object):

    parameters = {}

    def __init__(self, options, settings):
        self.options = options.get('options', {})
        self.notifiers = NotifiersList(settings)

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
        if self.option_has_default(option_name):
            return self.find_default(option_name)
        return default

    def option_has_default(self, option_name):
        for parameter_name, parameter_info in self.parameters.iteritems():
            if option_name == parameter_name and 'default' in parameter_info:
                return True

    def find_default(self, option_name):
        for parameter_name, parameter_info in self.parameters.iteritems():
            if option_name == parameter_name and 'default' in parameter_info:
                return parameter_info['default']
