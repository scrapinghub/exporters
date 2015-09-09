import json
import os
import uuid
from exporters.export_managers.unified_exporter import UnifiedExporter
from exporters.integration_tests.configs import readers, filters, transforms, writers, formatters
import time
import collections


def generate_config(test):
    config = {}
    config['exporter_options'] = {}
    config['exporter_options']['LOG_LEVEL'] = 'INFO'
    config['exporter_options']['LOGGER_NAME'] = 'export-pipeline'
    config['exporter_options']['RESUME'] = False
    config['exporter_options']['JOB_ID'] = ''
    config['exporter_options']['FORMATTER'] = test.formatter
    config['exporter_options']['NOTIFICATIONS'] = []

    config['reader'] = {}
    config['reader']['name'] = test.reader['name']
    config['reader']['options'] = test.reader['options']

    config['writer'] = {}
    config['writer']['name'] = test.writer['name']
    config['writer']['options'] = test.writer['options']

    config['filter'] = {}
    config['filter']['name'] = test.filter['name']
    config['filter']['options'] = test.filter['options']

    config['transform'] = {}
    config['transform']['name'] = test.transform['name']
    config['transform']['options'] = test.transform['options']

    config['persistence'] = {}
    config['persistence']['name'] = 'exporters.persistence.pickle_persistence.PicklePersistence'
    config['persistence']['options'] = {}
    config['persistence']['options']['file_path'] = '/tmp/'

    return json.dumps(config)


Test = collections.namedtuple('Test', 'reader filter transform writer formatter')


if __name__ == '__main__':
    total_tests = 0
    failed_tests = []
    successful_tests = []
    tests = [Test(r,f, t, w, fo) for r in readers for f in filters for t in transforms for w in writers for fo in formatters]

    for test in tests:
        json_config = generate_config(test)
        file_path = '/tmp/'+str(uuid.uuid4())+'.json'
        with open(file_path, 'w') as config_file:
            config_file.write(json_config)
        print 'START THE DUMP WITH CONFIG: '
        print test
        try:
            start = time.time()
            export_manager = UnifiedExporter.from_file_configuration(file_path)
            export_manager.run_export()
            elapsed = (time.time() - start)
            successful_tests.append({'test': test, 'elapsed_time': elapsed})
        except Exception as e:
            elapsed = (time.time() - start)
            failed_tests.append({'test': test, 'elapsed_time': elapsed, 'exception': e})
        print 'Elapsed time: {} seconds'.format(elapsed)
        print 'REMOVE TMP FILE'
        print '-----------------------------------'
        os.remove(file_path)

    if failed_tests:
        print 'FAILED_TESTS:'
        for t in failed_tests:
            print 'Test configuration: {}'.format(t['test'])
            print 'Elapsed time: {} seconds'.format(t['elapsed_time'])
            print 'Fail message: {}'.format(t['exception'])
            print '-----------------------------------'

    print 'TEST REPORT:'
    print '{} succesful tests'.format(len(successful_tests))
    print '{} failed tests'.format(len(failed_tests))

