from importlib import import_module
from exporters.exceptions import ConfigurationError


class ModuleLoader(object):

    def load_reader(self, options, **kwargs):
        from exporters.readers.base_reader import BaseReader
        return self._load_module(options, BaseReader, **kwargs)

    def load_filter(self, options, **kwargs):
        from exporters.filters.base_filter import BaseFilter
        return self._load_module(options, BaseFilter, **kwargs)

    def load_transform(self, options, **kwargs):
        from exporters.transform.base_transform import BaseTransform
        return self._load_module(options, BaseTransform, **kwargs)

    def load_writer(self, options, **kwargs):
        from exporters.writers.base_writer import BaseWriter
        return self._load_module(options, BaseWriter, **kwargs)

    def load_persistence(self, options, **kwargs):
        from exporters.persistence.base_persistence import BasePersistence
        return self._load_module(options, BasePersistence, **kwargs)

    def load_formatter(self, options, **kwargs):
        from exporters.export_formatter.base_export_formatter import BaseExportFormatter
        return self._load_module(options, BaseExportFormatter, **kwargs)

    def load_notifier(self, options, **kwargs):
        from exporters.notifications.base_notifier import BaseNotifier
        return self._load_module(options, BaseNotifier, **kwargs)

    def load_grouper(self, options, **kwargs):
        from exporters.groupers.base_grouper import BaseGrouper
        return self._load_module(options, BaseGrouper, **kwargs)

    def load_stats_manager(self, options, **kwargs):
        from exporters.stats_managers.base_stats_manager import BaseStatsManager
        return self._load_module(options, BaseStatsManager, **kwargs)

    def _instantiate_class(self, class_name, options, **kwargs):
        class_path_list = class_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        class_instance = getattr(mod, class_path_list[-1])(options, **kwargs)
        return class_instance

    def _load_module(self, options, module_type, **kwargs):
        module_name = options['name']
        try:
            instance = self._instantiate_class(module_name, options, **kwargs)
        except ConfigurationError as e:
            raise ConfigurationError('Error in configuration for module %s: %s' % (module_name, e))
        if not isinstance(instance, module_type):
            raise TypeError('Module must inherit from ' + str(module_type))
        return instance
