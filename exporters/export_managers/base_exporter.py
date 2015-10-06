from copy import deepcopy
import datetime
import traceback
from exporters.export_managers.bypass import RequisitesNotMet
from exporters.logger.base_logger import ExportManagerLogger
from exporters.notifications.notifiers_list import NotifiersList
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from exporters.writers.base_writer import ItemsLimitReached


class BaseExporter(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self.config = ExporterConfig(configuration)
        self.logger = ExportManagerLogger(self.config.log_options)
        self.module_loader = ModuleLoader()
        self.reader = self.module_loader.load_reader(self.config.reader_options)
        self.filter_before = self.module_loader.load_filter(
            self.config.filter_before_options)
        self.filter_after = self.module_loader.load_filter(
            self.config.filter_after_options)
        self.transform = self.module_loader.load_transform(self.config.transform_options)
        self.writer = self.module_loader.load_writer(self.config.writer_options)
        self.persistence = self.module_loader.load_persistence(
            self.config.persistence_options)
        self.export_formatter = self.module_loader.load_formatter(
            self.config.formatter_options)
        self.grouper = self.module_loader.load_grouper(self.config.grouper_options)
        self.notifiers = NotifiersList(self.config.notifiers)
        self.logger.debug('{} has been initiated'.format(self.__class__.__name__))
        self.stats_manager = self.module_loader.load_stats_manager(
            self.config.stats_options)
        job_info = {
            'configuration': self.secure_configuration,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.stats_manager.stats = job_info

    def _secure_regular_modules(self):
        secure_config = deepcopy(self.configuration)
        supported_options = {'reader': self.reader.supported_options,
                             'writer': self.writer.supported_options,
                             'persistence': self.persistence.supported_options,
                             'stats_manager': self.stats_manager.supported_options,
                             'filter_before': self.filter_before.supported_options,
                             'filter_after': self.filter_after.supported_options,
                             'transform': self.transform.supported_options,
                             'grouper': self.grouper.supported_options,
                             'export_formatter': self.export_formatter.supported_options
                             }
        for module, s_options in supported_options.iteritems():
            for key, supported_option in s_options.iteritems():
                if 'secret' in supported_option:
                    secure_config[module]['options'].pop(key)
        return secure_config

    def _secure_notifiers(self):
        secure_config = deepcopy(
            self.configuration.get('exporter_options', {}).get('notifications', []))
        notifiers = {notifier.__module__ + '.' + notifier.__class__.__name__:
                         notifier.supported_options for notifier in self.notifiers.notifiers}
        for i, n in enumerate(secure_config):
            for key, supported_option in notifiers[n['name']].iteritems():
                if 'secret' in supported_option:
                    secure_config[i]['options'].pop(key)
        return secure_config

    @property
    def secure_configuration(self):
        secure_config = self._secure_regular_modules()
        secure_config['exporter_options']['notifications'] = self._secure_notifiers()
        return secure_config

    def _run_pipeline_iteration(self):
        self.logger.debug('Getting new batch')
        next_batch = self.reader.get_next_batch()
        last_position = self.reader.get_last_position()
        next_batch = self.filter_before.filter_batch(next_batch)
        next_batch = self.transform.transform_batch(next_batch)
        next_batch = self.filter_after.filter_batch(next_batch)
        next_batch = self.grouper.group_batch(next_batch)
        next_batch = self.export_formatter.format(next_batch)
        self.writer.write_batch(batch=next_batch)
        self.persistence.commit_position(last_position)

    def _init_export_job(self):
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM],
                                         info=self.stats_manager.stats)
        last_position = self.persistence.get_last_position()
        self.reader.set_last_position(last_position)

    def _clean_export_job(self):
        self.persistence.delete_instance()
        self.writer.close_writer()

    def _finish_export_job(self):
        self.stats_manager.stats['items_count'] = self.writer.items_count
        self.stats_manager.stats['end_time'] = datetime.datetime.now()
        self.stats_manager.stats['elapsed_time'] = self.stats_manager.stats['end_time'] - \
                                                   self.stats_manager.stats['start_time']

    @property
    def bypass_cases(self):
        return []

    def bypass_exporter(self, bypass_script):
        self.logger.info('Executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM],
                                         info=self.stats_manager.stats)
        bypass_script.bypass()
        self.logger.info(
            'Finished executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM],
                                            info=self.stats_manager.stats)

    def bypass(self):
        for bypass_script in self.bypass_cases:
            try:
                bypass_script.meets_conditions()
                self.bypass_exporter(bypass_script)
                return True
            except RequisitesNotMet:
                self.logger.debug(
                    '{} bypass skipped.'.format(bypass_script.__class__.__name__))
        return False

    def _handle_export_exception(self, exception):
        self.logger.error(traceback.format_exc(exception))
        self.logger.error(str(exception))
        self.notifiers.notify_failed_job(str(exception),
                                         str(traceback.format_exc(exception)),
                                         receivers=[TEAM], info=self.stats_manager.stats)

    def _run_pipeline(self):
        while not self.reader.is_finished():
            try:
                self._run_pipeline_iteration()
            except ItemsLimitReached as e:
                self.logger.info('{!r}'.format(e))
                break
            else:
                self.stats_manager.populate()

    def export(self):
        if not self.bypass():
            try:
                self._init_export_job()
                self._run_pipeline()
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM],
                                                    info=self.stats_manager.stats)
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
                self._finish_export_job()
        self.logger.info(str(self.stats_manager.stats))
