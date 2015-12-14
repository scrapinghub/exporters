import datetime
import traceback
from exporters.writers.base_writer import ItemsLimitReached
from exporters.export_managers import MODULES
from exporters.export_managers.bypass import RequisitesNotMet
from exporters.logger.base_logger import ExportManagerLogger
from exporters.notifications.notifiers_list import NotifiersList
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.notifications.receiver_groups import CLIENTS, TEAM


class BaseExporter(object):
    def __init__(self, configuration):
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
        job_info = {
            'configuration': configuration,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.stats_manager = self.module_loader.load_stats_manager(
            self.config.stats_options)
        self.stats_manager.stats = job_info
        self.bypass_cases = []

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
        self.writer.close()

    def _finish_export_job(self):
        self.stats_manager.stats['items_count'] = self.writer.items_count
        self.stats_manager.stats['end_time'] = datetime.datetime.now()
        self.stats_manager.stats['elapsed_time'] = self.stats_manager.stats['end_time'] - \
                                                   self.stats_manager.stats['start_time']

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
        if self.config.prevent_bypass:
            return False

        for bypass_script in self.bypass_cases:
            try:
                bypass_script.meets_conditions()
                self.persistence.close()
                self.persistence.delete()
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

    def _update_stats(self):
        for mod in MODULES:
            self.stats_manager.update_module_stats(mod, getattr(self, mod).stats)

    def _populate_stats(self):
        try:
            self.stats_manager.populate()
        except Exception as e:
            self.logger.error('Error populating stats: {}'.format(str(e)))

    def _run_pipeline(self):
        while not self.reader.is_finished():
            try:
                self._run_pipeline_iteration()
            except ItemsLimitReached as e:
                self.logger.info('{!r}'.format(e))
                break
            else:
                self._update_stats()
                self._populate_stats()
        self.writer.flush()

    def export(self):
        if not self.bypass():
            try:
                self._init_export_job()
                self._run_pipeline()
                self._finish_export_job()
                self._update_stats()
                self._populate_stats()
                self.persistence.close()
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM],
                                                    info=self.stats_manager.stats)
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
        self.logger.info(str(self.stats_manager.stats))
