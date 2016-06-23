.. _Github repository: https://github.com/scrapinghub/ozzy/

Ozzy project documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

Ozzy provide a flexible way to export data from multiple sources to
multiple destinations, allowing filtering and transforming the data.

This `Github repository`_ is used as a central repository.


Getting Started
===============

Install ozzy
------------

First of all, we recommend to create a virtualenv::

    virtualenv ozzy
    source ozzy/bin/activate

..

Ozzy can be cloned from its `Github repository`_::

    git clone git@github.com:scrapinghub/ozzy.git

..

Then, we install the requirements::

    cd ozzy
    pip install -r requirements.txt

..

Creating a configuration
------------------------

Then, we can create our first configuration object and store it in a file called config.json.
 This configuration will read from an s3 bucket and store it in our filesystem, exporting only
 the records which have United States in field country:

.. code-block:: javascript

   {
        "reader": {
            "name": "ozzy.readers.s3_reader.S3Reader",
            "options": {
                "bucket": "YOUR_BUCKET",
                "aws_access_key_id": "YOUR_ACCESS_KEY",
                "aws_secret_access_key": "YOUR_SECRET_KEY",
                "prefix": "ozzy-tutorial/sample-dataset"
            }
        },
        "filter": {
            "name": "ozzy.filters.key_value_regex_filter.KeyValueRegexFilter",
            "options": {
                "keys": [
                    {"name": "country", "value": "United States"}
                ]
            }
        },
        "writer":{
            "name": "ozzy.writers.fs_writer.FSWriter",
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

The export can be run using ozzy as a library:

.. code-block:: python

    from ozzy.export_managers.basic_exporter import BasicExporter

    exporter = BasicExporter.from_file_configuration('config.json')
    exporter.export()


Resuming an export job
----------------------

Let's suppose we have a pickle file with a previously failed export job. If we want to resume it
we must run the export script:

.. code-block:: shell

    python bin/export.py --resume pickle://pickle-file.pickle
