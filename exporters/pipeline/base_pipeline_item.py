from exporters.notifications.notifiers_list import NotifiersList


class BasePipelineItem(object):
    def __init__(self, options, settings):
        self.options = options.get('options', {})
        self.notifiers = NotifiersList(settings)

    def check_options(self):
        for requirement_name, requirement_info in self.requirements.iteritems():
            requirement_value = self.read_option(requirement_name)
            if requirement_value and not isinstance(requirement_value, requirement_info['type']):
                raise ValueError('Parameter ' + requirement_name + ' should be type: ' + str(requirement_info['type']))
            if not requirement_info['required']:
                continue
            if not requirement_value:
                raise ValueError('Options object should have parameter: ' + requirement_name)

    def read_option(self, option_name, default=None):
        if option_name in self.options:
            return self.options.get(option_name)
        if self.option_has_default(option_name):
            return self.find_default(option_name)
        return default

    def option_has_default(self, option_name):
        for requirement_name, requirement_info in self.requirements.iteritems():
            if option_name == requirement_name and 'default' in requirement_info:
                return True

    def find_default(self, option_name):
        for requirement_name, requirement_info in self.requirements.iteritems():
            if option_name == requirement_name and 'default' in requirement_info:
                return requirement_info['default']
