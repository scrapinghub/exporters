.. _Github repository: https://github.com/scrapinghub/exporters/

Exporters project documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exporters provide a flexible way to export data from multiple sources to
multiple destinations, allowing filtering and transforming the data.

This `Github repository`_ is used as a central repository.

Full documentation can be found here http://exporters.readthedocs.io/en/latest/


Getting Started
===============

Install exporters
-----------------

First of all, we recommend to create a virtualenv::

    virtualenv exporters
    source exporters/bin/activate

..

Installing::

    pip install exporters

..



Creating a configuration
------------------------

Then, we can create our first configuration object and store it in a file called config.json.
 This configuration will read from an s3 bucket and store it in our filesystem, exporting only
 the records which have United States in field country:

.. code-block:: javascript

   {
        "reader": {
            "name": "exporters.readers.s3_reader.S3Reader",
            "options": {
                "bucket": "YOUR_BUCKET",
                "aws_access_key_id": "YOUR_ACCESS_KEY",
                "aws_secret_access_key": "YOUR_SECRET_KEY",
                "prefix": "exporters-tutorial/sample-dataset"
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
        "writer":{
            "name": "exporters.writers.fs_writer.FSWriter",
            "options": {
                "filebase": "/tmp/output/"
            }
        }
   }


Export with script
------------------

We can use the provided script to run this export:

.. code-block:: shell

    python bin/export.py --config config.json


Use it as a library
-------------------

The export can be run using exporters as a library:

.. code-block:: python

    from exporters import BasicExporter

    exporter = BasicExporter.from_file_configuration('config.json')
    exporter.export()


Resuming an export job
----------------------

Let's suppose we have a pickle file with a previously failed export job. If we want to resume it
we must run the export script:

.. code-block:: shell

    python bin/export.py --resume pickle://pickle-file.pickle
