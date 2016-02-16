from importlib import import_module
from exporters.exceptions import ConfigurationError


class ModuleLoader(object):

    def __init__(self, export_metadata):
        self.export_metadata = export_metadata

    def load_reader(self, options):
        from exporters.readers.base_reader import BaseReader
        return self._load_module(options, BaseReader)

    def load_filter(self, options):
        from exporters.filters.base_filter import BaseFilter
        return self._load_module(options, BaseFilter)

    def load_transform(self, options):
        from exporters.transform.base_transform import BaseTransform
        return self._load_module(options, BaseTransform)

    def load_writer(self, options):
        from exporters.writers.base_writer import BaseWriter
        return self._load_module(options, BaseWriter)

    def load_persistence(self, options):
        from exporters.persistence.base_persistence import BasePersistence
        return self._load_module(options, BasePersistence)

    def load_formatter(self, options):
        from exporters.export_formatter.base_export_formatter import BaseExportFormatter
        return self._load_module(options, BaseExportFormatter)

    def load_notifier(self, options):
        from exporters.notifications.base_notifier import BaseNotifier
        return self._load_module(options, BaseNotifier)

    def load_grouper(self, options):
        from exporters.groupers.base_grouper import BaseGrouper
        return self._load_module(options, BaseGrouper)

    def load_stats_manager(self, options):
        from exporters.stats_managers.base_stats_manager import BaseStatsManager
        return self._load_module(options, BaseStatsManager)

    def _instantiate_class(self, class_name, options):
        class_path_list = class_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        class_instance = getattr(mod, class_path_list[-1])(options)
        return class_instance

    def _load_module(self, options, module_type):
        module_name = options['name']
        try:
            instance = self._instantiate_class(module_name, options)
            instance.export_metadata = self.export_metadata
        except ConfigurationError as e:
            raise ConfigurationError('Error in configuration for module %s: %s' % (module_name, e))
        if not isinstance(instance, module_type):
            raise TypeError('Module must inherit from ' + str(module_type))
        return instance
