import datetime
import traceback
from exporters.export_managers.bypass import RequisitesNotMet
from exporters.export_managers.settings import Settings
from exporters.logger.base_logger import ExportManagerLogger
from exporters.notifications.notifiers_list import NotifiersList
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from exporters.writers.base_writer import ItemsLimitReached


class BaseExporter(object):

    def __init__(self, configuration):
        self.config = ExporterConfig(configuration)
        self.settings = Settings(self.config.exporter_options)
        self.logger = ExportManagerLogger(self.settings)
        self.module_loader = ModuleLoader()
        self.reader = self._create_reader(self.config.reader_options)
        self.filter_before = self._create_filter(self.config.filter_before_options)
        self.filter_after = self._create_filter(self.config.filter_after_options)
        self.transform = self._create_transform(self.config.transform_options)
        self.writer = self._create_writer(self.config.writer_options)
        self.persistence = self._create_persistence(self.config.persistence_options)
        self.export_formatter = self._create_formatter(self.config.formatter_options)
        self.grouper = self._create_grouper(self.config.grouper_options)
        self.notifiers = NotifiersList(self.config.notifiers)
        self.logger.debug('{} has been initiated'.format(self.__class__.__name__))
        job_info = {
            'configuration': configuration,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }
        self.stats_manager = self._create_stats_manager(self.config.stats_options)
        self.stats_manager.stats = job_info

    def _create_reader(self, options):
        reader = self.module_loader.load_reader(options)
        reader.set_configuration(self.config)
        return reader

    def _create_transform(self, options):
        transform = self.module_loader.load_transform(options)
        transform.set_configuration(self.config)
        return transform

    def _create_filter(self, options):
        efilter = self.module_loader.load_filter(options)
        efilter.set_configuration(self.config)
        return efilter

    def _create_writer(self, options):
        writer = self.module_loader.load_writer(options)
        writer.set_configuration(self.config)
        return writer

    def _create_persistence(self, options):
        persistence = self.module_loader.load_persistence(options)
        persistence.set_configuration(self.config)
        return persistence

    def _create_formatter(self, options):
        formatter = self.module_loader.load_formatter(options)
        formatter.set_configuration(self.config)
        return formatter

    def _create_grouper(self, options):
        grouper = self.module_loader.load_grouper(options)
        grouper.set_configuration(self.config)
        return grouper

    def _create_stats_manager(self, options):
        return self.module_loader.load_stats_manager(options)

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
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM], info=self.stats_manager.stats)
        last_position = self.persistence.get_last_position()
        self.reader.set_last_position(last_position)

    def _clean_export_job(self):
        self.persistence.delete_instance()
        self.writer.close_writer()

    def _finish_export_job(self):
        self.stats_manager.stats['items_count'] = self.writer.items_count
        self.stats_manager.stats['end_time'] = datetime.datetime.now()
        self.stats_manager.stats['elapsed_time'] = self.stats_manager.stats['end_time'] - self.stats_manager.stats['start_time']

    @property
    def bypass_cases(self):
        return []

    def bypass_exporter(self, bypass_script):
        self.logger.info('Executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM], info=self.stats_manager.stats)
        bypass_script.bypass()
        self.logger.info('Finished executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM], info=self.stats_manager.stats)

    def bypass(self):
        for bypass_script in self.bypass_cases:
            try:
                bypass_script.meets_conditions()
                self.bypass_exporter(bypass_script)
                return True
            except RequisitesNotMet:
                self.logger.debug('{} bypass skipped.'.format(bypass_script.__class__.__name__))
        return False

    def _handle_export_exception(self, exception):
        self.logger.error(traceback.format_exc(exception))
        self.logger.error(str(exception))
        self.notifiers.notify_failed_job(str(exception), str(traceback.format_exc(exception)), receivers=[TEAM], info=self.stats_manager.stats)

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
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM], info=self.stats_manager.stats)
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
                self._finish_export_job()
        self.logger.info(str(self.stats_manager.stats))
