.. _exporters:

Exporters description
=====================

What are exporters?
-------------------

Exporters are a project aiming to provide a flexible and
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

- exporter_options (mandatory): it contains general export options, including output format.
- reader (mandatory): defines what reader module the export should use and its options.
- writer (mandatory): defines what writer module the export should use and its options.
- filter_before: defines what filter module should be used before transforming and its options.
- filter_after: defines what filter module should be used after transforming and its options.
- transform: defines what transform module the export should use and its options.
- persistence: defines what persistence module the export should use and its options.
- stats_manager: defines what stats_manager module the export should use and its options.
- grouper: defines what grouper module the export should use and its options.

This is an example of the simplest config file that can be used.

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


Modules
-------
Export Manager
~~~~~~~~~~~~~~
This module is in charge of the pipeline iteration, and it is the one executed to start it. It must call the reader to
get a batch, call the transform module, and finally write and commit the batch. It is also in charge of notifications
and retries management.

Provided exporters
******************

BasicExporter
#############
.. automodule:: exporters.export_managers.basic_export_manager
    :members:
    :undoc-members:
    :show-inheritance:


Bypass support
**************
Exporters arqchitecture provides support to bypass the pipeline. A usage example of that is the case in which both reader
and writer aim S3 buckets. If no transforms or filtering are needed, keys can be copied directly without downloading them.

All bypass classes are subclasses of BaseBypass class, and must implement two methods:

    - meets_conditions(configuration)
            Checks if provided export configuration meets the requirements to use the bypass. If not, a RequisitesNotMet
            exception must be thrown.

    - run()
        Executes the bypass script

Provided Bypass scripts
***********************
    - S3Bypass

Reader
~~~~~~
.. automodule:: exporters.readers.base_reader
    :members:
    :undoc-members:
    :show-inheritance:


Provided readers
****************
RandomReader
############
.. automodule:: exporters.readers.random_reader
    :members:
    :undoc-members:
    :show-inheritance:

KafkaScannerReader
##################
.. automodule:: exporters.readers.kafka_scanner_reader
    :members:
    :undoc-members:
    :show-inheritance:

KafkaRandomReader
#################
.. automodule:: exporters.readers.kafka_random_reader
    :members:
    :undoc-members:
    :show-inheritance:

S3Reader
########
.. automodule:: exporters.readers.s3_reader
    :members:
    :undoc-members:
    :show-inheritance:


HubstorageReader
################
.. automodule:: exporters.readers.hubstorage_reader
    :members:
    :undoc-members:
    :show-inheritance:


Writer
~~~~~~
.. automodule:: exporters.writers.base_writer
    :members:
    :undoc-members:
    :show-inheritance:


Provided writers
****************
ConsoleWriter
#############
.. automodule:: exporters.writers.console_writer
    :members:
    :undoc-members:
    :show-inheritance:

S3Writer
########
.. automodule:: exporters.writers.s3_writer
    :members:
    :undoc-members:
    :show-inheritance:


FTPWriter
#########
.. automodule:: exporters.writers.ftp_writer
    :members:
    :undoc-members:
    :show-inheritance:


HubstorageWriter
################
.. automodule:: exporters.writers.hubstorage_writer
    :members:
    :undoc-members:
    :show-inheritance:


FSWriter
########
.. automodule:: exporters.writers.fs_writer
    :members:
    :undoc-members:
    :show-inheritance:


MailWriter
##########
.. automodule:: exporters.writers.mail_writer
    :members:
    :undoc-members:
    :show-inheritance:


AggregationWriter
#################
.. automodule:: exporters.writers.aggregation_writer
    :members:
    :undoc-members:
    :show-inheritance:


CloudsearchWriter
#################
.. automodule:: exporters.writers.cloudsearch_writer
    :members:
    :undoc-members:
    :show-inheritance:


GDriveWriter
############
.. automodule:: exporters.writers.gdrive_writer
    :members:
    :undoc-members:
    :show-inheritance:



Transform
~~~~~~~~~
.. automodule:: exporters.transform.base_transform
    :members:
    :undoc-members:
    :show-inheritance:

Provided transform
******************
NoTransform
###########
.. automodule:: exporters.transform.no_transform
    :members:
    :undoc-members:
    :show-inheritance:


JqTransform
###########
.. automodule:: exporters.transform.jq_transform
    :members:
    :undoc-members:
    :show-inheritance:

PythonexpTransform
##################
.. automodule:: exporters.transform.pythonexp_transform
    :members:
    :undoc-members:
    :show-inheritance:

Filter
~~~~~~
.. automodule:: exporters.filters.base_filter
    :members:
    :undoc-members:
    :show-inheritance:

Provided filters
****************

NoFilter
########
.. automodule:: exporters.filters.no_filter
    :members:
    :undoc-members:
    :show-inheritance:

KeyValueFilter
##############
.. automodule:: exporters.filters.key_value_filter
    :members:
    :undoc-members:
    :show-inheritance:

KeyValueRegex
#############
.. automodule:: exporters.filters.key_value_regex_filter
    :members:
    :undoc-members:
    :show-inheritance:

PythonExpeRegex
###############
.. automodule:: exporters.filters.pythonexp_filter
    :members:
    :undoc-members:
    :show-inheritance:


Persistence
~~~~~~~~~~~
.. automodule:: exporters.persistence.base_persistence
    :members:
    :undoc-members:
    :show-inheritance:


Provided persistence
********************
PicklePersistence
#################
.. automodule:: exporters.persistence.pickle_persistence
    :members:
    :undoc-members:
    :show-inheritance:


AlchemyPersistence
##################
.. automodule:: exporters.persistence.alchemy_persistence
    :members:
    :undoc-members:
    :show-inheritance:


Notifications
~~~~~~~~~~~~~
.. automodule:: exporters.notifications.base_notifier
    :members:
    :undoc-members:
    :show-inheritance:


Provided notifications
**********************
SESMailNotifier
###############
.. automodule:: exporters.notifications.s3_mail_notifier
    :members:
    :undoc-members:
    :show-inheritance:


Grouping
~~~~~~~~
.. automodule:: exporters.groupers.base_grouper
    :members:
    :undoc-members:
    :show-inheritance:


Provided Groupers
*****************
KeyFileGrouper
##############
.. automodule:: exporters.groupers.file_key_grouper
    :members:
    :undoc-members:
    :show-inheritance: