.. _tutorials:

Exporters Tutorial
==================

In this tutorial, we are going to learn how the new exporters work. The purpose of this project is to have a tool to
allow us to export from a wide range of sources to a wide range of targets, allowing to perform complex filtering and transformations to items.


Let's make an export
~~~~~~~~~~~~~~~~~~~~
With this tutorial, we are going to export indeed companies collection to a s3 bucket, filtering by country of
the company (We only want American companies). We need to create the proper configuration file. To do so, you can both
create a plain json file with the proper data, or to use the `config generator
<http://ds-dev.dc21.scrapinghub.com:8000/app/>`_. we have developed.


Config file
***********
Configuration file should be something like:

.. code-block:: javascript

    {
        "exporter_options": {
            "log_level": "DEBUG",
            "logger_name": "export-pipeline",
            "formatter": {
                "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                "options": {}
            },
            "notifications":[
            ]
        },
        "reader": {
            "name": "exporters.readers.hubstorage_reader.HubstorageReader",
            "options": {
                "apikey": "88bd7f6607b54b11ad5972e6db03454d",
                "project_id": "10843",
                "collection_name": "companies",
                "batch_size": 10000
            }
        },
        "filter": {
            "name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
            "options": {
                "keys": [
                    {"name": "country", "value": "United States"}
                ]
            }
        },
        "writer": {
            "name": "exporters.writers.s3_writer.S3Writer",
            "options": {
                "bucket": "datasets.scrapinghub.com",
                "filebase": "tests/export_tutorial_{:%d-%b-%Y}",
                "aws_access_key_id": "AKIAJ6VP76KAK7UOUWEQ",
                "aws_secret_access_key": "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"
            }
        },
        "persistence": {
            "name": "exporters.persistence.pickle_persistence.PicklePersistence",
            "options": {
              "file_path": "/tmp/"
            }
        }
    }


If we save this file as ~/config.json, the export job would be launched with the following command:

.. code-block:: python

    python bin/export.py --config ~/config.json



