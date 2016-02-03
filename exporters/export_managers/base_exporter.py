import datetime
import time
import traceback
from collections import OrderedDict
from exporters.writers.base_writer import ItemsLimitReached
from exporters.export_managers import MODULES
from exporters.export_managers.base_bypass import RequisitesNotMet
from exporters.logger.base_logger import ExportManagerLogger
from exporters.notifications.notifiers_list import NotifiersList
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.notifications.receiver_groups import CLIENTS, TEAM

times = OrderedDict([('started', datetime.datetime.now())])


def time_profile(f):
    def f_timer(*args, **kwargs):
        result = f(*args, **kwargs)
        times.update({args[2]['name']: datetime.datetime.now()})
        return result
    return f_timer


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
            'accurate_items_count': True,
            'start_time': datetime.datetime.now(),
            'script_name': 'export'
        }
        self.stats_manager = self.module_loader.load_stats_manager(
            self.config.stats_options)
        self.stats_manager.stats = job_info
        self.bypass_cases = []

        self.pipeline_steps = [
            {
                'name': 'read',
                'method': self.reader.get_next_batch,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'filter_before',
                'method': self.filter_before.filter_batch,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'transform',
                'method': self.transform.transform_batch,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'filter_before',
                'method': self.filter_after.filter_batch,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'group',
                'method': self.grouper.group_batch,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'formatted',
                'method': self.export_formatter.format,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'write',
                'method': self.writer.write_batch,
                'exception': ItemsLimitReached,
                'exception_handler': self._handle_items_limit
            },
            {
                'name': 'stats',
                'method': self._make_and_publish_stats,
                'exception': None,
                'exception_handler': None
            },
            {
                'name': 'persistence',
                'method': self._update_and_commit_last_position,
                'exception': None,
                'exception_handler': None
            },
        ]



    def _handle_items_limit(self, exception):
        raise exception

    def _make_and_publish_stats(self):
        self.stats_manager.stats['items_count'] = self.writer.items_count

    def _update_and_commit_last_position(self):
        last_position = self.reader.get_last_position()
        last_position['items_count'] = self.writer.items_count
        last_position['accurate_items_count'] = self.stats_manager.stats['accurate_items_count']
        self.persistence.commit_position(last_position)

    @time_profile
    def _execute_step(self, batch, step, exception=None):
        if exception:
            try:
                batch = self._execute_step(batch, step)
            except exception as e:
                step['exception_handler'](e)
        else:
            if batch:
                batch = step['method'](batch)
            else:
                batch = step['method']()
        return batch

    def _run_pipeline_iteration(self):
        batch = None
        for step in self.pipeline_steps:
            batch = self._execute_step(batch, step, step['exception'])
        self._iteration_stats_report()

    def _init_export_job(self):
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM],
                                         info=self.stats_manager.stats)
        last_position = self.persistence.get_last_position()
        if last_position is not None:
            items_count = last_position.get('items_count', 0)
            self.writer.items_count = items_count
            self.stats_manager.stats['items_count'] = items_count
            self.stats_manager.stats['accurate_items_count'] = last_position.get('accurate_items_count', False)
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
        if not self.config.exporter_options.get('resume'):
            self.persistence.close()
            self.persistence.delete()
        bypass_script.bypass()
        if not bypass_script.valid_total_count:
            self.stats_manager.stats['accurate_items_count'] = False
            self.logger.warning('No accurate items count info can be retrieved')
        self.stats_manager.stats['items_count'] += bypass_script.total_items
        self.logger.info(
            'Finished executing bypass {}.'.format(bypass_script.__class__.__name__))
        self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM], info=self.stats_manager.stats)

    def bypass(self):
        if self.config.prevent_bypass:
            return False

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

    def _collect_stats(self):
        return {mod: getattr(self, mod).stats for mod in MODULES}

    def _iteration_stats_report(self):
        try:
            stats = self._collect_stats()
            self.stats_manager.iteration_report(times, stats)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.logger.error('Error making stats report: {}'.format(str(e)))

    def _final_stats_report(self):
        try:
            stats = self._collect_stats()
            self.stats_manager.final_report(stats)
        except Exception as e:
            self.logger.error('Error making final stats report: {}'.format(str(e)))

    def _run_pipeline(self):
        while not self.reader.is_finished():
            try:
                self._run_pipeline_iteration()
            except ItemsLimitReached as e:
                self.logger.info('{!r}'.format(e))
                break
        self.writer.flush()

    def export(self):
        if not self.bypass():
            try:
                self._init_export_job()
                self._run_pipeline()
                self._finish_export_job()
                self._final_stats_report()
                self.persistence.close()
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM],
                                                    info=self.stats_manager.stats)
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
        self.logger.info(str(self.stats_manager.stats))
