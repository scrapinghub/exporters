class BasePipelineItem(object):

    base_supported_options = {}
    supported_options = {}

    def __new__(cls, options):
        obj = object.__new__(cls)
        object.__init__(obj, options)
        obj.supported_options = cls.base_supported_options.copy()
        obj.supported_options.update(cls.supported_options)
        return obj

    def __init__(self, options):
        self.options = options.get('options', {})
        self.check_options()

    def check_options(self):
        for supported_option_name, supported_option_info in self.supported_options.iteritems():
            supported_option_value = self.read_option(supported_option_name)
            if supported_option_value and not isinstance(supported_option_value, supported_option_info['type']):
                raise ValueError('Parameter ' + supported_option_name + ' should be type: ' + str(supported_option_info['type']))
            if 'default' in supported_option_info:
                continue
            if not supported_option_value:
                raise ValueError('Options object should have supported_option: ' + supported_option_name)

    def read_option(self, option_name, default=None):
        if option_name in self.options:
            return self.options.get(option_name)
        return self.supported_options.get(option_name, {}).get('default', default)