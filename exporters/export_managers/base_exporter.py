import datetime
import traceback
from exporters.export_managers.bypass import RequisitesNotMet
from exporters.export_managers.settings import Settings
from exporters.logger.base_logger import ExportManagerLogger
from exporters.notifications.notifiers_list import NotifiersList
from exporters.module_loader import ModuleLoader
from exporters.exporter_options import ExporterOptions
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from exporters.writers.base_writer import ItemsLimitReached


def lower_keys(tree):
    if not isinstance(tree, dict):
        return tree
    return {k.lower(): lower_keys(v) for k, v in tree.items()}


class BaseExporter(object):

    def __init__(self, configuration):
        configuration = lower_keys(configuration)
        exporter_options = ExporterOptions(configuration)
        self.configuration = configuration
        self.settings = Settings(exporter_options.exporter_options)
        self.logger = ExportManagerLogger(self.settings)
        self.module_loader = ModuleLoader()
        self.reader = self._create_reader(exporter_options.reader_options)
        self.filter_before = self._create_filter(exporter_options.filter_before_options)
        self.filter_after = self._create_filter(exporter_options.filter_after_options)
        self.transform = self._create_transform(exporter_options.transform_options)
        self.writer = self._create_writer(exporter_options.writer_options)
        self.persistence = self._create_persistence(exporter_options)
        self.export_formatter = self._create_formatter(exporter_options.formatter_options)
        self.grouper = self._create_grouper(exporter_options.grouper_options)
        self.notifiers = NotifiersList(self.settings)
        self.logger.debug('{} has been initiated'.format(self.__class__.__name__))
        self.job_info = {
            'configuration': configuration,
            'items_count': 0,
            'start_time': datetime.datetime.now(),
            'script_name': 'basic_export_manager'
        }

    def _create_reader(self, options):
        return self.module_loader.load_reader(options, self.settings)

    def _create_transform(self, options):
        return self.module_loader.load_transform(options, self.settings)

    def _create_filter(self, options):
        return self.module_loader.load_filter(options, self.settings)

    def _create_writer(self, options):
        return self.module_loader.load_writer(options, self.settings)

    def _create_persistence(self, options):
        return self.module_loader.load_persistence(options, self.settings)

    def _create_formatter(self, options):
        return self.module_loader.load_formatter(options, self.settings)

    def _create_grouper(self, options):
        return self.module_loader.load_grouper(options, self.settings)

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
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM], info=self.job_info)
        last_position = self.persistence.get_last_position()
        self.reader.set_last_position(last_position)

    def _clean_export_job(self):
        self.persistence.delete_instance()
        self.writer.close_writer()

    def _finish_export_job(self):
        self.job_info['items_count'] = self.writer.items_count
        self.job_info['end_time'] = datetime.datetime.now()
        self.job_info['elapsed_time'] = self.job_info['end_time'] - self.job_info['start_time']

    def populate_stats(self):
        self.logger.info(str(self.job_info))

    @property
    def bypass_cases(self):
        return []

    def bypass_exporter(self, bypass_script):
        self.logger.info('Executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM], info=self.job_info)
        bypass_script.bypass()
        self.logger.info('Finished executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM], info=self.job_info)

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
        self.notifiers.notify_failed_job(str(exception), str(traceback.format_exc(exception)), receivers=[TEAM], info=self.job_info)

    def _run_pipeline(self):
        while not self.reader.is_finished():
            try:
                self._run_pipeline_iteration()
            except ItemsLimitReached as e:
                self.logger.info('{!r}'.format(e))
                break
            else:
                self.populate_stats()

    def export(self):
        if not self.bypass():
            try:
                self._init_export_job()
                self._run_pipeline()
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM], info=self.job_info)
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
                self._finish_export_job()
        self.logger.info(str(self.job_info))
