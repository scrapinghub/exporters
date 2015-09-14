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
        return self.parameters.get(option_name, {}).get('default', default)
