.. _tutorials:

Exporters Tutorial
==================

In this tutorial, we are going to learn how the new exporters work. The purpose of this project is to have a tool to
allow us to export from a wide range of sources to a wide range of targets, allowing to perform complex filtering and transformations to items.


Exporters API tutorial
----------------------
Main entry point to make exports with dataservices infrastructure is exporters script, placed under bin/export.py.


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
            "LOG_LEVEL": "DEBUG",
            "LOGGER_NAME": "export-pipeline",
            "RESUME": false,
            "JOB_ID": "",
            "FORMATTER": {
                "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                "options": {}
            },
            "NOTIFICATIONS":[
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

    python bin/export.py --useapi --label "tutorial" ~/config.json

This will work if SHUB_APIKEY is set as environment variable in your shell. Otherwise, you must add the option `--apikey`
with your dash apikey to be able to schedule the job. `--label` option will name the job created in  `Dataservices management project
<https://staging.scrapinghub.com/p/7389/jobs/>`_.

Some theory
~~~~~~~~~~~
The whole architecture is based on the idea of batches. A reader reads a batch and passes it to the pipeline. Here you can take a look at the architecture:

.. image:: _images/exporters_pipe.png
   :scale: 60 %
   :alt: Exporters architecture
   :align: center

.. note::
    Note that we still have a pending discussion about groupers. So, it will not be shown in this basic tutorial.


Local usage
-----------
Install
~~~~~~~
First of all, we recommend to create a virtualenv::

    virtualenv exporters
    source exporters/bin/activate

..

Exporters are part of dataservices repository (they will be splitted soon). So, let's clone it::

    git clone git@github.com:scrapinghub/dataservices.git

..

Then, we install the requirements::

    cd dataservices
    pip install -r requirements.txt

..

And, finally, we install the dataservices package::

    python setup.py install

..

After that, we can start using the dataservices package.


Let's make an export
~~~~~~~~~~~~~~~~~~~~
With this tutorial, we are going to export indeed companies collection to our filesystem, filtering by country code of
the company (We only want english companies). We need to create the proper configuration object.

.. code-block:: python

    export_configuration = {}


Adding a reader
***************
We need the hubstorage reader with proper configuration:

.. code-block:: python

    export_configuration['reader'] = {
        'name': 'exporters.readers.hubstorage_reader.HubstorageReader',
        'options': {
            # A valid api key with access to the project
            'apikey': '88bd7f6607b54b11ad5972e6db03454d',
            # Collection project's id
            'project_id': '10843',
            # The name of the collection to export
            'collection_name': 'companies',
            # How many items do we want to read in each pipeline iteration.
            'batch_size': 10000
        }
    }


Adding a filter
***************
We need a filter supporting key/value basic filtering. We can use the KeyValueRegexFilter:

.. code-block:: python

    export_configuration['filter'] = {
        'name': 'exporters.filters.key_value_regex_filter.KeyValueRegexFilter',
        'options': {
            # We can use more than one filter
            'keys': [
                # With this dict, we just tell him to filter all the items which as a country_code that does not match
                # GB regex
                {'name': 'country', 'value': 'United States'}
            ]
        }
    }

Adding a transform
******************
We don't need to transform items, so we can just use NoTransform module:

.. code-block:: python

    export_configuration['transform'] = {
        'name': 'exporters.transform.no_transform.NoTransform',
        'options': {

        }
    }

Adding a persistence module
***************************
Persistence module handles resuming support. If a job has a problem, and the process dies, we can resume the export job
thanks to this module. As this is just a demo, we can use the PicklePersistence module.

.. code-block:: python

    export_configuration['persistence'] = {
        'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
        'options': {
          # Where to keep the pickle file.
          'file_path': '/tmp/'
        }
    }

Adding a writer
***************
We need the FSWriter:

.. code-block:: python

    export_configuration['writer'] = {
        'name': 'exporters.writers.fs_writer.FSWriter',
        'options': {
            # Where to place the exported files
            'filebase': '/tmp/output',
            # Folder to store tmp files
            'tmp_folder': '/tmp'
        }
    }

Adding general options
**********************
Exporters also need to be aware of some options, such as notifiers modules and so on.

.. code-block:: python

    export_configuration['exporter_options'] = {
        'LOG_LEVEL': 'DEBUG',
        'LOGGER_NAME': 'export-pipeline',
        # Wether to try to resume a job or not
        'RESUME': False,
        # If RESUME is se to True, we must specify the id of the job to resume.
        'JOB_ID': '',
        # Export format. Let's keep it json lines.
        'FORMATTER': {
            'name': 'exporters.export_formatter.json_export_formatter.JsonExportFormatter',
            'options': {}
        },
        # Notifications module. It supports an array of notificators. (emails, webhooks...)
        'NOTIFICATIONS':[

        ]
    }

Show me the code!
~~~~~~~~~~~~~~~~~
To start the job, we must create an export manager, pass the created configuration and call the loop.

.. code-block:: python

    from exporters.export_managers.unified_manager import UnifiedExporter
    export_manager = UnifiedExporter(export_configuration)
    export_manager.run_export()



We also provide a script to perform exports. We can store the configuration in json format in a file, and run the export
job by calling that script:

.. code-block:: shell

    python bin/export.py --config PATHTOFILE


We can also use exports api to store and provide configurations under the endpoint https://datahub-exports-api.scrapinghub.com/configurations

.. code-block:: shell

    python bin/export.py --config https://datahub-exports-api.scrapinghub.com/configurations/CONFIG_ID/detail

