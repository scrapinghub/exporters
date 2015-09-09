# Readers
random_reader = {
    'name': 'exporters.readers.random_reader.RandomReader',
    'options': {
        'number_of_items': 10000,
        'batch_size': 1000
    }
}

hs_reader = {
    'name': 'exporters.readers.hubstorage_reader.HubstorageReader',
    'options': {
        'apikey': '88bd7f6607b54b11ad5972e6db03454d',
        'project_id': '15702',
        'collection_name': 'properties',
        'batch_size': 500
    }
}

kafka_reader = {
    'name': 'exporters.readers.kafka_reader.KafkaReader',
    'options': {
        'batch_size': 10000,
        'brokers': ['kafka1.dc21.scrapinghub.com:9092', 'kafka1.dc21.scrapinghub.com:9092', 'kafka1.dc21.scrapinghub.com:9092'],
        'topic': 'indeed-companies-items',
        'group': 'Scrapinghub'
    }
}

s3_reader = {
    'name': 'exporters.readers.s3_reader.S3Reader',
    'options': {
        'bucket': 'datasets.scrapinghub.com',
        'aws_access_key_id': 'AKIAJ6VP76KAK7UOUWEQ',
        'aws_secret_access_key': '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',
        'tmp_folder': '/tmp',
        'prefix': 'test/indeed/companies-wrapped/10-Jun-2015',
        'batch_size': 10000
    }
}

readers = [hs_reader, s3_reader]

# Writers
console_writer = {
    'name': 'exporters.writers.console_writer.ConsoleWriter',
    'options': {

    },
    'export_format': 'json'
}

fs_writer = {
    'name': 'exporters.writers.fs_writer.FSWriter',
    'options': {
        'filebase': '/tmp/output',
        'tmp_folder': '/tmp'
    },
    'export_format': 'json'
}

s3_writer = {
    'name': 'exporters.writers.s3_writer.S3Writer',
    'options': {
        'bucket': 'datasets.scrapinghub.com',
        'aws_access_key_id': 'AKIAJ6VP76KAK7UOUWEQ',
        'aws_secret_access_key': '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',
        'filebase': 'tests/export_pipelines/'
    },
    'export_format': 'json'
}

writers = [fs_writer, s3_writer]


# Transforms
jq_transform = {
    'name': 'exporters.transform.jq_transform.JQTransform',
    'options': {
        'jq_filter': '.'
    }
}

pythonexp_transform = {
    'name': 'exporters.transform.pythonexp_transform.PythonexpTransform',
    'options': {
        'python_expressions':
        [
            "item.update({'new_field': 1})",
        ]
    }
}

no_transform = {
    'name': 'exporters.transform.no_transform.NoTransform',
    'options': {

    }
}

transforms = [jq_transform, pythonexp_transform]


# Filters
key_value_filter = {
    'name': 'exporters.filters.key_value_filter.KeyValueFilter',
    'options': {
        'keys': [
            {'name': 'country_code', 'value': 'es'}
        ]
    }
}

key_value_regex_filter = {
    'name': 'exporters.filters.key_value_regex_filter.KeyValueRegexFilter',
    'options': {
        'keys': [
            {'name': 'country_code', 'value': 'e'}
        ]
    }
}

no_filter = {
    'name': 'exporters.filters.no_filter.NoFilter',
    'options': {

    }
}

python_exp_filter = {
    'name': 'exporters.filters.pythonexp_filter.PythonexpFilter',
    'options': {
        'python_expression': "'country_code' in item"
    }
}

filters = [key_value_filter, key_value_regex_filter, python_exp_filter]


# Formatters
json = {
    'name': 'exporters.export_formatter.json_export_formatter.JsonExportFormatter',
    'options': {}
}

csv = {
    'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
    'options': {}
}

formatters = [json, csv]