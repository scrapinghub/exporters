.. _exporters:

Exporters description
=====================

What are exporters?
-------------------

Exporters is a project aiming to provide a flexible and
easy to extend infrastructure to export data from multiple sources to multiple
destinations, allowing filtering and transforming the data.


Architecture
------------

.. image:: _images/exporters_pipe.png
   :scale: 60 %
   :alt: Exporters architecture
   :align: center


Config file
-----------

Exporters behaviour is defined by what we call a configuration object. This object has the
following sections:

- reader (mandatory): defines what reader module the export should use and its options.
- writer (mandatory): defines what writer module the export should use and its options.
- exporter_options: it contains general export options, including output format.
- filter_before: defines what filter module should be used before transforming and its options.
- filter_after: defines what filter module should be used after transforming and its options.
- transform: defines what transform module the export should use and its options.
- persistence: defines what persistence module the export should use and its options.
- stats_manager: defines what stats_manager module the export should use and its options.
- grouper: defines what grouper module the export should use and its options.

This is an example of the simplest config file that can be used.

.. code-block:: javascript

    {

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
