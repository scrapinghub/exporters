from importlib import import_module
from exporters.exceptions import ConfigurationError


class ModuleLoader(object):

    def load_reader(self, options, metadata, **kwargs):
        from exporters.readers.base_reader import BaseReader
        return self._load_module(options, metadata, BaseReader, **kwargs)

    def load_filter(self, options, metadata, **kwargs):
        from exporters.filters.base_filter import BaseFilter
        return self._load_module(options, metadata, BaseFilter, **kwargs)

    def load_transform(self, options, metadata, **kwargs):
        from exporters.transform.base_transform import BaseTransform
        return self._load_module(options, metadata, BaseTransform, **kwargs)

    def load_writer(self, options, metadata, **kwargs):
        from exporters.writers.base_writer import BaseWriter
        return self._load_module(options, metadata, BaseWriter, **kwargs)

    def load_persistence(self, options, metadata, **kwargs):
        from exporters.persistence.base_persistence import BasePersistence
        return self._load_module(options, metadata, BasePersistence, **kwargs)

    def load_formatter(self, options, metadata, **kwargs):
        from exporters.export_formatter.base_export_formatter import BaseExportFormatter
        return self._load_module(options, metadata, BaseExportFormatter, **kwargs)

    def load_notifier(self, options, metadata, **kwargs):
        from exporters.notifications.base_notifier import BaseNotifier
        return self._load_module(options, metadata, BaseNotifier, **kwargs)

    def load_grouper(self, options, metadata, **kwargs):
        from exporters.groupers.base_grouper import BaseGrouper
        return self._load_module(options, metadata, BaseGrouper, **kwargs)

    def load_stats_manager(self, options, metadata, **kwargs):
        from exporters.stats_managers.base_stats_manager import BaseStatsManager
        return self._load_module(options, metadata, BaseStatsManager, **kwargs)

    def _instantiate_class(self, class_name, options, metadata, **kwargs):
        class_path_list = class_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        class_instance = getattr(mod, class_path_list[-1])(options, metadata, **kwargs)
        return class_instance

    def _load_module(self, options, metadata, module_type, **kwargs):
        module_name = options['name']
        try:
            instance = self._instantiate_class(module_name, options, metadata, **kwargs)
        except ConfigurationError as e:
            raise ConfigurationError('Error in configuration for module %s: %s' % (module_name, e))
        if not isinstance(instance, module_type):
            raise TypeError('Module must inherit from ' + str(module_type))
        return instance
