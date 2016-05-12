import json
from exporters.defaults import DEFAULT_FILTER_CONFIG, DEFAULT_TRANSFORM_CONFIG, \
    DEFAULT_GROUPER_CONFIG, DEFAULT_PERSISTENCE_CONFIG, DEFAULT_STATS_MANAGER_CCONFIG, \
    DEFAULT_LOGGER_LEVEL, DEFAULT_FORMATTER_CONFIG
from exporters.exporter_config import module_options, _get_module_supported_options


DEFAULT_CONFIG_FILE_NAME = 'config.json'
MODULE_DEFAULTS = {
    'readers': 'exporters.readers.random_reader.RandomReader',
    'writers': 'exporters.writers.console_writer.ConsoleWriter',
    'transform': 'exporters.transform.no_transform.NoTransform',
    'persistence': 'exporters.persistence.pickle_persistence.PicklePersistence',
    'filters': 'exporters.filters.no_filter.NoFilter',
    'stats_managers': 'exporters.stats_managers.basic_stats_manager.BasicStatsManager',
    'groupers': 'exporters.groupers.no_grouper.NoGrouper',
    'export_formatter': 'exporters.export_formatter.json_export_formatter.JsonExportFormatter'
}

MODULE_TRANSLATION = {
    'reader': 'readers',
    'writer': 'writers',
    'transform': 'transform',
    'persistence': 'persistence',
    'filter_after': 'filters',
    'filter_before': 'filters',
    'stats_manager': 'stats_managers',
    'grouper': 'groupers',
    'formatter': 'export_formatter'
}

VALID_LOG_LEVELS = ('INFO', 'DEBUG', 'WARN', 'ERROR')


def get_input(prompt, default=None):
    text = raw_input('{} '.format(prompt))
    if not text:
        text = default
    return text


def get_module_choices(module_type):
    modules = module_options()[module_type]
    names = [module['name'] for module in modules]
    choices = {i: name for i, name in enumerate(names)}
    return choices


def get_module_text(section, module_type, choices):
    text = 'Select a {}: \n'.format(section) + '\n'.join(
            ['{} - {}'.format(k, v+' (default)' if v == MODULE_DEFAULTS[module_type] else v)
             for k, v in choices.iteritems()]) + '\n'
    return text


def get_module_choice_number(module_type, choice):
    if choice is not None:
        return get_module_choices(module_type)[int(choice)]
    return MODULE_DEFAULTS[module_type]


def get_supported_option_text(supported_option, params):
    text = '{} {} ({}): '.format(
            supported_option, params.get('type'), params.get('default', 'required'))
    if params.get('help'):
        text += '\n' + params.get('help') + '\n'
    return text


def parse_value(option_value, option_type):
    if option_value is None:
        return
    try:
        if option_type in (list, object):
            parsed = eval(option_value)
            if type(parsed) is not option_type:
                raise
            return parsed
        return option_type(option_value)
    except:
        raise ValueError('{} is not of type {}'.format(option_value, option_type))


def get_section_info(section):
    section_choices = get_module_choices(MODULE_TRANSLATION[section])
    section_text = get_module_text(section, MODULE_TRANSLATION[section], section_choices)
    section = get_module_choice_number(MODULE_TRANSLATION[section], get_input(section_text))
    section_supported_options = _get_module_supported_options(section)
    section_options = {}
    for supported_option, params in section_supported_options.iteritems():
        text = get_supported_option_text(supported_option, params)
        option_value = get_input(text)
        if option_value:
            section_options[supported_option] = parse_value(option_value, params['type'])
    return {'name': section, 'options': section_options}


def curate_configuration(configuration):
    if configuration['filter_after'] == DEFAULT_FILTER_CONFIG:
        configuration.pop('filter_after')
    if configuration['filter_before'] == DEFAULT_FILTER_CONFIG:
        configuration.pop('filter_before')
    if configuration['transform'] == DEFAULT_TRANSFORM_CONFIG:
        configuration.pop('transform')
    if configuration['grouper'] == DEFAULT_GROUPER_CONFIG:
        configuration.pop('grouper')
    if configuration['persistence'] == DEFAULT_PERSISTENCE_CONFIG:
        configuration.pop('persistence')
    if configuration['stats_manager'] == DEFAULT_STATS_MANAGER_CCONFIG:
        configuration.pop('stats_manager')
    if configuration['formatter'] == DEFAULT_FORMATTER_CONFIG:
        configuration.pop('formatter')
    else:
        configuration['exporter_options']['formatter'] = configuration['formatter']
        configuration.pop('formatter')
    return configuration


def get_exporter_options():
    exporter_options = {}
    log_level = get_input(
            'Select log level ({}): '.format(DEFAULT_LOGGER_LEVEL), DEFAULT_LOGGER_LEVEL)
    if log_level not in VALID_LOG_LEVELS:
        raise ValueError('Log level must be one of '.format(DEFAULT_LOGGER_LEVEL))
    exporter_options['log_level'] = log_level
    return exporter_options


def create_config():
    configuration = {}

    file_name = get_input('Please enter config file name ({}): '.format(
            DEFAULT_CONFIG_FILE_NAME), DEFAULT_CONFIG_FILE_NAME)

    for section, _ in MODULE_TRANSLATION.items():
        section_info = get_section_info(section)
        configuration[section] = section_info

    configuration['exporter_options'] = get_exporter_options()
    configuration = curate_configuration(configuration)

    with open(file_name, 'w') as f:
        f.write(json.dumps(configuration, indent=2, sort_keys=True))
