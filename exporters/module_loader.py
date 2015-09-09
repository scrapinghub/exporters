from importlib import import_module


class ModuleLoader(object):

    def load_reader(self, options, settings):
        from exporters.readers.base_reader import BaseReader
        return self._load_module(options, settings, BaseReader)

    def load_filter(self, options, settings):
        from exporters.filters.base_filter import BaseFilter
        return self._load_module(options, settings, BaseFilter)

    def load_transform(self, options, settings):
        from exporters.transform.base_transform import BaseTransform
        return self._load_module(options, settings, BaseTransform)

    def load_writer(self, options, settings):
        from exporters.writers.base_writer import BaseWriter
        return self._load_module(options, settings, BaseWriter)

    def load_persistence(self, options, settings):
        from exporters.persistence.base_persistence import BasePersistence
        return self._load_persistence_module(options, settings, BasePersistence)

    def load_formatter(self, options, settings):
        from exporters.export_formatter.base_export_formatter import BaseExportFormatter
        return self._load_module(options, settings, BaseExportFormatter)

    def load_notifier(self, options, settings):
        from exporters.notifications.base_notifier import BaseNotifier
        return self._load_module(options, settings, BaseNotifier)

    def load_grouper(self, options, settings):
        from exporters.groupers.base_grouper import BaseGrouper
        return self._load_module(options, settings, BaseGrouper)

    def _load_class(self, class_name, options, settings):
        class_path_list = class_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        class_instance = getattr(mod, class_path_list[-1])(options, settings)
        return class_instance

    # Load an exporter module
    def _load_module(self, options, settings, module_type):
        module_name = options['name']
        class_instance = self._load_class(module_name, options, settings)
        if not isinstance(class_instance, module_type):
            raise TypeError('Module must inherit from ' + str(module_type.__class__))
        return class_instance

    def _load_persistence_module(self, options, settings, module_type):
        grouper_name = options.persistence_options['name']
        class_instance = self._load_class(grouper_name, options, settings)
        if not isinstance(class_instance, module_type):
            raise TypeError('Module must inherit from ' + str(module_type.__class__))
        return class_instance

