import os
import pickle
import shutil
import tempfile

import mock
import unittest
from mock import DEFAULT
from exporters.export_managers.base_exporter import BaseExporter
from exporters.export_managers.basic_exporter import BasicExporter
from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.readers.random_reader import RandomReader
from exporters.transform.no_transform import NoTransform
from exporters.utils import TmpFile, TemporaryDirectory
from exporters.writers.console_writer import ConsoleWriter
from .utils import valid_config_with_updates, ErrorWriter, CopyingMagicMock


def get_filename(path, persistence_id):
    return os.path.join(path, persistence_id)


def fail(*a, **kw):
    raise ValueError("fail")


class FakeBypass(BaseBypass):
    executed = False
    fake_meet_conditions = True

    def __init__(self, options, metadata=None):
        super(FakeBypass, self).__init__(options, metadata)

    @classmethod
    def meets_conditions(cls, config):
        if not cls.fake_meet_conditions:
            raise RequisitesNotMet

    def execute(self):
        self.__class__.executed = True


class BaseExportManagerTest(unittest.TestCase):
    def build_config(self, **kwargs):
        defaults = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 1
                }
            }
        }
        defaults.update(**kwargs)
        return valid_config_with_updates(defaults)

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if hasattr(self, 'exporter'):
            self.exporter.persistence.delete()
        shutil.rmtree(self.tmp_dir)

    def test_simple_export(self):
        self.exporter = exporter = BaseExporter(self.build_config())
        exporter.export()
        self.assertEquals(10, exporter.writer.get_metadata('items_count'))

    def test_simple_grouped_export(self):
        config = self.build_config(
            writer={
                'name': 'exporters.writers.fs_writer.FSWriter',
                'options': {
                    'filebase': os.path.join(self.tmp_dir, '{file_number}_ds_dump_{groups[0]}'),
                    'items_per_buffer_write': 100,
                }
            },
            grouper={
                'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
                'options': {
                    'keys': ['state'],
                }
            }
        )
        config['reader']['options']['number_of_items'] = 1000
        self.exporter = exporter = BaseExporter(config)
        exporter.export()
        used_paths = [value['path_safe_keys'][0]
                      for key, value in exporter.writer.write_buffer.grouping_info.iteritems()]
        regex = '\d_ds_dump_({})'.format('|'.join(used_paths))
        written_files = [os.path.basename(f) for f in exporter.writer.written_files.keys()]

        for w_file in written_files:
            self.assertRegexpMatches(w_file, regex)

    def test_grouping_with_non_valid_characters(self):
        config = self.build_config(
            writer={
                'name': 'exporters.writers.fs_writer.FSWriter',
                'options': {
                    'filebase': os.path.join(self.tmp_dir, '{file_number}_ds_dump_{groups[0]}'),
                    'items_per_buffer_write': 100,
                }
            },
            grouper={
                'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
                'options': {
                    'keys': ['country_code'],
                }
            }
        )
        config['reader']['options']['number_of_items'] = 1000
        expected_written_files = set([
            '0_ds_dump_us.jl.gz',
            '0_ds_dump_es.jl.gz',
            '0_ds_dump_uk.jl.gz',
        ])
        self.exporter = exporter = BaseExporter(config)
        exporter.export()
        written_files = [os.path.basename(f) for f in exporter.writer.written_files.keys()]
        self.assertContainsSubset(written_files, expected_written_files)

    def assertContainsSubset(self, iterable, subset):
        """Check if iterable first argument contains
        all items in second argument
        """
        a, b = set(iterable), set(subset)
        self.assertTrue(a & b == b, "%r is not a subset of %r" % (b, a))

    def test_export_with_csv_formatter(self):
        config = self.build_config()
        config['exporter_options']['formatter'] = {
            'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
            'options': {
                'show_titles': True,
                'fields': ['city', 'country_code']
            }
        }
        self.exporter = exporter = BaseExporter(config)
        exporter.export()
        expected_count = 10
        self.assertEquals(expected_count, exporter.writer.get_metadata('items_count'))

    def test_bypass_should_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())

        class Bypass(FakeBypass):
            executed = False
        exporter.bypass_cases = [Bypass]

        # when:
        exporter.export()
        exporter.writer.close()

        # then:
        self.assertTrue(Bypass.executed, "Bypass should have been called")

    def test_when_unmet_conditions_bypass_should_not_be_called(self):
        # given:
        self.exporter = exporter = BaseExporter(self.build_config())

        class Bypass(FakeBypass):
            executed = False
        Bypass.fake_meet_conditions = False
        exporter.bypass_cases = [Bypass]

        # when:
        exporter.export()

        # then:
        self.assertFalse(Bypass.executed, "Bypass should NOT have been called")

    def test_when_meet_conditions_but_config_prevent_bypass_it_should_not_be_called(self):
        # given:
        config = self.build_config()
        config['exporter_options']['prevent_bypass'] = True
        self.exporter = exporter = BaseExporter(config)

        class Bypass(FakeBypass):
            executed = False
        exporter.bypass_cases = [Bypass]

        # when:
        exporter.export()

        # then:
        self.assertFalse(Bypass.executed, "Bypass should NOT have been called")

    @mock.patch('exporters.writers.ftp_writer.FTPWriter.write', new=fail)
    @mock.patch('exporters.export_managers.base_exporter.NotifiersList')
    def test_when_writing_only_on_flush_should_notify_job_failure(self, mock_notifier):
        config = self.build_config(
            writer={
                'name': 'exporters.writers.FTPWriter',
                'options': {
                    'host': 'ftp.invalid.com',
                    'filebase': '_',
                    'ftp_user': 'invaliduser',
                    'ftp_password': 'invalidpass',
                }
            },
        )
        self.exporter = exporter = BaseExporter(config)
        with self.assertRaisesRegexp(ValueError, "fail"):
            exporter.export()
        self.assertFalse(mock_notifier.return_value.notify_complete_dump.called,
                         "Should not notify a successful dump")
        self.assertTrue(mock_notifier.return_value.notify_failed_job.called,
                        "Should notify the job failure")

    def test_resume_items(self):
        with TmpFile() as pickle_file:
            # given:
            persistence_data = {
                'last_position': {
                    'accurate_items_count': False,
                    'writer_metadata': {'items_count': 30},
                    'last_key': 3
                },
            }
            pickle.dump(persistence_data, open(pickle_file, 'w'))

            config = self.build_config(
                exporter_options={
                    'resume': True,
                    'persistence_state_id': os.path.basename(pickle_file),
                },
                persistence={
                    'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                    'options': {
                        'file_path': os.path.dirname(pickle_file)
                    }
                }
            )

            # when:
            self.exporter = exporter = BaseExporter(config)
            exporter._init_export_job()

            # then:
            self.assertEqual(30, exporter.writer.get_metadata('items_count'))
            self.assertFalse(exporter.metadata.accurate_items_count,
                             "Couldn't get accurate count from last_position")
            exporter.writer.close()

    def test_read_goes_into_written(self):
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 123,
                    'batch_size': 7
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter'
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        self.exporter = exporter = BaseExporter(options)
        exporter.export()
        self.assertEquals(exporter.reader.get_metadata('read_items'),
                          exporter.writer.get_metadata('items_count'),
                          msg="Export should write the same number items as it has read")

    def test_fs_grouping_without_filebase(self):
        with TemporaryDirectory() as tmp_dir:
            options = {
                'reader': {
                    'name': 'exporters.readers.random_reader.RandomReader',
                    'options': {
                        'number_of_items': 123,
                        'batch_size': 7
                    }
                },
                'writer': {
                    'name': 'exporters.writers.fs_writer.FSWriter',
                    'options': {
                        'filebase': tmp_dir+'/some_file_'
                    }
                },
                'persistence': {
                    'name': 'tests.utils.NullPersistence',
                },
                'grouper': {
                    'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
                    'options': {
                        'keys': ['country_code']
                    }
                }
            }
            self.exporter = exporter = BaseExporter(options)
            exporter.export()
            self.assertEqual(len(os.listdir(tmp_dir)), 3, 'There should be a file for every group')

    def test_notifier_integration_ok(self):
        notifier_class_path = 'exporters.notifications.base_notifier.BaseNotifier'
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter'
            },
            'exporter_options': {
                'notifications': [
                    {
                        'name': notifier_class_path
                    }
                ]
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        with mock.patch.multiple(notifier_class_path,
                                 notify_start_dump=DEFAULT,
                                 notify_failed_job=DEFAULT,
                                 notify_complete_dump=DEFAULT) as m:
            self.exporter = exporter = BaseExporter(options)
            exporter.export()
            self.assertEquals(len(m['notify_start_dump'].mock_calls), 1,
                              msg='There was 1 start dump notification')
            self.assertEquals(len(m['notify_failed_job'].mock_calls), 0,
                              msg='There should be no failed job notification')
            self.assertEquals(len(m['notify_complete_dump'].mock_calls), 1,
                              msg='There was 1 complete dump notification')

    def test_notifier_integration_export_fail(self):
        notifier_class_path = 'exporters.notifications.base_notifier.BaseNotifier'
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.ErrorWriter'
            },
            'exporter_options': {
                'notifications': [
                    {
                        'name': notifier_class_path
                    }
                ]
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        with mock.patch.multiple(notifier_class_path,
                                 notify_start_dump=DEFAULT,
                                 notify_failed_job=DEFAULT,
                                 notify_complete_dump=DEFAULT) as m:
            self.exporter = exporter = BaseExporter(options)
            with self.assertRaisesRegexp(RuntimeError, ErrorWriter.msg):
                exporter.export()
                exporter.writer.close()
            self.assertEquals(len(m['notify_start_dump'].mock_calls), 1,
                              msg='There was 1 start dump notification')
            self.assertEquals(len(m['notify_failed_job'].mock_calls), 1,
                              msg='There was 1 failed job notification')
            _, args, _ = m['notify_failed_job'].mock_calls[0]
            self.assertEquals(args[0], ErrorWriter.msg)
            self.assertEquals(len(m['notify_complete_dump'].mock_calls), 0,
                              msg='There should be no complete dump notification')

    def test_valid_bypass(self):
        notifier_class_path = 'exporters.notifications.base_notifier.BaseNotifier'
        options = {
            'reader': {
                # error reader and writer should give
                # failed test if they get used
                'name': 'tests.utils.ErrorReader'
            },
            'writer': {
                'name': 'tests.utils.ErrorWriter'
            },
            'exporter_options': {
                'notifications': [
                    {
                        'name': notifier_class_path
                    }
                ]
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        with mock.patch.multiple(notifier_class_path,
                                 notify_start_dump=DEFAULT,
                                 notify_failed_job=DEFAULT,
                                 notify_complete_dump=DEFAULT) as m:
            self.exporter = exporter = BaseExporter(options)

            class Bypass(BaseBypass):
                @classmethod
                def meets_conditions(cls, config):
                    pass

                def execute(self):
                    pass

            exporter.bypass_cases.append(Bypass)
            exporter.export()
            exporter.writer.close()
            self.assertGreaterEqual(
                len, 2,
                msg=('bypass class should have at least `meets_conditions` '
                     'and `bypass` methods called'))
            self.assertEquals(len(m['notify_start_dump'].mock_calls), 1,
                              msg='There was 1 start dump notification')
            self.assertEquals(len(m['notify_failed_job'].mock_calls), 0,
                              msg='There should be no failed job notification')
            self.assertEquals(len(m['notify_complete_dump'].mock_calls), 1,
                              msg='There was 1 complete dump notification')

    def test_skipped_bypass(self):
        class SkippedBypass(BaseBypass):
            @classmethod
            def meets_conditions(cls, config):
                raise RequisitesNotMet

        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 10,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter'
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        self.exporter = exporter = BaseExporter(options)
        exporter.bypass_cases.append(SkippedBypass)
        exporter.export()
        self.assertEqual(exporter.reader.get_metadata('read_items'), 10,
                         msg='There should be 10 read items')
        self.assertEqual(exporter.writer.get_metadata('items_count'), 10,
                         msg='There should be 10 written items')

    def test_writer_items_limit(self):
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 17,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter',
                'options': {
                    'items_limit': 5
                }
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            }
        }
        self.exporter = exporter = BaseExporter(options)
        exporter.export()
        self.assertEqual(exporter.writer.get_metadata('items_count'), 5,
                         msg='There should be only 5 written items')

    @mock.patch("mock.MagicMock", new=CopyingMagicMock)
    def test_persisted_positions(self):
        pers_class_path = 'tests.utils.NullPersistence'
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 17,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter'
            },
            'persistence': {
                'name': pers_class_path,
            }
        }
        self.exporter = exporter = BaseExporter(options)
        with mock.patch.object(exporter.persistence, 'commit_position') as m:
            exporter.export()
            last_read = [args[0]['last_read'] for name, args, kwargs in m.mock_calls]
            self.assertEqual(last_read, [2, 5, 8, 11, 14, 16])

    def test_disabling_retries(self):
        count_holder = [0]
        options = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 17,
                    'batch_size': 3
                }
            },
            'writer': {
                'name': 'tests.utils.NullWriter',
                'options': {
                    'items_limit': 5
                }
            },
            'persistence': {
                'name': 'tests.utils.NullPersistence',
            },
            'exporter_options': {
                'notifications': [
                    {
                        'name': 'exporters.notifications.webhook_notifier.WebhookNotifier',
                        'options': {
                            'endpoints': ['http://endpoint']
                        }
                    }
                ],
                'disable_retries': True
            },
        }

        def count_and_raise(*args, **kwargs):
            count_holder[0] += 1
            raise RuntimeError('test exception')

        self.exporter = exporter = BaseExporter(options)
        with mock.patch('requests.post', side_effect=count_and_raise):
            exporter.export()

        # there should be 2 posts: for started and completed dump
        self.assertEqual(count_holder[0], 2, "Retries should be disabled")


class BasicExportManagerTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline',
                'formatter': {
                    'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
                    'options': {
                        'show_titles': True,
                        'fields': ['city', 'country_code']
                    }
                }
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {

                }
            }
        }

    def test_parses_the_options_and_loads_pipeline_items(self):
        exporter = BasicExporter(self.options)
        try:
            self.assertTrue(isinstance(exporter.reader, RandomReader))
            self.assertTrue(isinstance(exporter.writer, ConsoleWriter))
            self.assertTrue(isinstance(exporter.transform, NoTransform))
            exporter._clean_export_job()
        finally:
            exporter.persistence.delete()

    def test_from_file_configuration(self):
        test_manager = BasicExporter.from_file_configuration('./tests/data/basic_config.json')
        try:
            self.assertIsInstance(test_manager, BasicExporter)
            test_manager._clean_export_job()
        finally:
            test_manager.persistence.delete()
