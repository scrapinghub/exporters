import datetime
import traceback
from collections import OrderedDict
from contextlib import closing
from exporters.default_retries import disable_retries
from exporters.exporter_config import ExporterConfig
from exporters.logger.base_logger import ExportManagerLogger
from exporters.meta import ExportMeta
from exporters.module_loader import ModuleLoader
from exporters.notifications.notifiers_list import NotifiersList
from exporters.notifications.receiver_groups import CLIENTS, TEAM
from exporters.writers.base_writer import ItemsLimitReached


class BaseExporter(object):
    def __init__(self, configuration):
        self.config = ExporterConfig(configuration)
        self.logger = ExportManagerLogger(self.config.log_options)
        self.module_loader = ModuleLoader()
        metadata = ExportMeta(configuration)
        self.metadata = metadata
        self.reader = self.module_loader.load_reader(
            self.config.reader_options, metadata)
        self.filter_before = self.module_loader.load_filter(
            self.config.filter_before_options, metadata)
        self.filter_after = self.module_loader.load_filter(
            self.config.filter_after_options, metadata)
        self.transform = self.module_loader.load_transform(
            self.config.transform_options, metadata)
        self.export_formatter = self.module_loader.load_formatter(
            self.config.formatter_options, metadata)
        self.writer = self.module_loader.load_writer(
            self.config.writer_options, metadata, export_formatter=self.export_formatter)
        self.persistence = self.module_loader.load_persistence(
            self.config.persistence_options, metadata)
        self.grouper = self.module_loader.load_grouper(
            self.config.grouper_options, metadata)
        self.notifiers = NotifiersList(self.config.notifiers, metadata)
        if self.config.disable_retries:
            disable_retries()
        self.logger.debug('{} has been initiated'.format(self.__class__.__name__))
        self.stats_manager = self.module_loader.load_stats_manager(
            self.config.stats_options, metadata)
        self.bypass_cases = []

    def _run_pipeline_iteration(self):
        times = OrderedDict([('started', datetime.datetime.now())])
        self.logger.debug('Getting new batch')
        if self.config.exporter_options.get('forced_reads'):
            next_batch = list(self.reader.get_next_batch())
        else:
            next_batch = self.reader.get_next_batch()
        times.update(read=datetime.datetime.now())
        next_batch = self.filter_before.filter_batch(next_batch)
        times.update(filtered=datetime.datetime.now())
        next_batch = self.transform.transform_batch(next_batch)
        times.update(transformed=datetime.datetime.now())
        next_batch = self.filter_after.filter_batch(next_batch)
        times.update(filtered_after=datetime.datetime.now())
        next_batch = self.grouper.group_batch(next_batch)
        times.update(grouped=datetime.datetime.now())
        try:
            self.writer.write_batch(batch=next_batch)
            times.update(written=datetime.datetime.now())
            last_position = self._get_last_position()
            self.persistence.commit_position(last_position)
            times.update(persisted=datetime.datetime.now())
        except ItemsLimitReached:
            # we have written some amount of records up to the limit
            times.update(written=datetime.datetime.now())
            self._iteration_stats_report(times)
            raise
        else:
            self._iteration_stats_report(times)

    def _get_last_position(self):
        last_position = self.reader.get_last_position()
        last_position['writer_metadata'] = self.writer.get_all_metadata()
        return last_position

    def _init_export_job(self):
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM])
        last_position = self.persistence.get_last_position()
        if last_position is not None:
            self.writer.update_metadata(last_position.get('writer_metadata'))
            self.metadata.accurate_items_count = last_position.get('accurate_items_count', False)
        self.reader.set_last_position(last_position)

    def _clean_export_job(self):
        try:
            self.reader.close()
        except:
            raise
        finally:
            self.writer.close()

    def _finish_export_job(self):
        self.writer.finish_writing()
        self.metadata.end_time = datetime.datetime.now()

    def bypass_exporter(self, bypass_class):
        self.logger.info('Executing bypass {}.'.format(bypass_class.__name__))
        self.notifiers.notify_start_dump(receivers=[CLIENTS, TEAM])
        if not self.config.exporter_options.get('resume'):
            self.persistence.close()
            self.persistence.delete()
        with closing(bypass_class(self.config, self.metadata)) as bypass:
            bypass.execute()
        if not bypass.valid_total_count:
            self.metadata.accurate_items_count = False
            self.logger.warning('No accurate items count info can be retrieved')
        self.writer.set_metadata(
            'items_count', self.writer.get_metadata('items_count') + bypass.total_items)
        self.logger.info(
            'Finished executing bypass {}.'.format(bypass_class.__name__))
        self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM])

    def bypass(self):
        if self.config.prevent_bypass:
            return False
        for bypass_class in self.bypass_cases:
            if bypass_class.meets_conditions(self.config):
                try:
                    self.bypass_exporter(bypass_class)
                    return True
                finally:
                    self._clean_export_job()
        return False

    def _handle_export_exception(self, exception):
        self.logger.error(traceback.format_exc(exception))
        self.logger.error(str(exception))
        self.notifiers.notify_failed_job(
            str(exception), str(traceback.format_exc(exception)), receivers=[TEAM])

    def _iteration_stats_report(self, times):
        try:
            self.stats_manager.iteration_report(times)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.logger.error('Error making stats report: {}'.format(str(e)))

    def _final_stats_report(self):
        try:
            self.stats_manager.final_report()
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
                self.notifiers.notify_complete_dump(receivers=[CLIENTS, TEAM])
            except Exception as e:
                self._handle_export_exception(e)
                raise e
            finally:
                self._clean_export_job()
        else:
            self.metadata.bypassed_pipeline = True
