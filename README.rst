.. _Github repository: https://github.com/scrapinghub/exporters/

Exporters project documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Exporters are a project aiming to provide a flexible and
easy to extend infrastructure to export data from multiple sources to multiple
destinations, allowing filtering and transforming the data.

This `Github repository`_ is used as a central repository.


Getting Started
===============

Install exporters
-----------------

First of all, we recommend to create a virtualenv::

    virtualenv exporters
    source exporters/bin/activate

..

Exporters are part of dataservices repository (they will be splitted soon). So, let's clone it::

    git clone git@github.com:scrapinghub/exporters.git

..

Then, we install the requirements::

    cd exporters
    pip install -r requirements.txt

..

Creating a configuration
------------------------

Then, we can create our first configuration object and store it in a file called config.json:

.. code-block:: javascript

   {
        "exporter_options":{
        },
        "reader": {
            "name": "exporters.readers.random_reader.RandomReader",
            "options": {
                "number_of_items": 1000,
                "batch_size": 10
            }
        },
        "writer":{
            "name": "exporters.writers.console_writer.ConsoleWriter",
            "options": {
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

    from exporters.export_managers.basic_exporter import BasicExporter

    exporter = BasicExporter.from_file_configuration('config.json')
    exporter.export()
