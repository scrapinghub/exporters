.. _exporters:

Exporters description
=====================

What are exporters?
-------------------

Dumpers has been rethought in order to have a more flexible, scalable and maintainable structure.
The main idea is to isolate each dumping phase (Reading, Writing, etc) via well-defined interfaces, making it easier
to mix, match them and extend them. This documentation will follow a json notation to configure the pipeline.


Architecture
------------

.. image:: _images/exporters_pipe.png
   :scale: 60 %
   :alt: Exporters architecture
   :align: center


Config file
-----------

This is an example of a config file using kafka reader and s3 writer.

.. code-block:: javascript

    {
       "exporter_options":{
           "log_level": "DEBUG",
           "logger_name": "export-pipeline",
           "formatter": {
                "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                "options":{}
           },
           "notifications":[

           ]
       },
       "reader": {
           "name": "exporters.readers.kafka_reader.KafkaReader",
           "options": {
               "batch_size": 10000,
               "brokers": ["kafka1.dc21.scrapinghub.com:9092", "kafka1.dc21.scrapinghub.com:9092", "kafka1.dc21.scrapinghub.com:9092"],
               "topic": "indeed-companies-items",
               "group": "Scrapinghub"
           }
       },
       "filter_before": {
           "name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
           "options": {
               "keys": [
                   {"name": "state", "value": "val"}
               ]
           }
       },
       "filter_after": {
           "name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
           "options": {
               "keys": [
                   {"name": "country_code", "value": "es"}
               ]
           }
       },
       "transform": {
           "name": "exporters.transform.no_transform.NoTransform",
           "options": {
           }
       },
       "writer":{
           "name": "exporters.writers.s3_writer.S3Writer",
           "grouper": {
               "name": "exporters.groupers.file_key_grouper.FileKeyGrouper",
               "options":{
                   "keys": ["country_code", "state", "city"]
               }
           },
           "options": {
               "aws_access_key_id": "AKIAJDGLM4HBWQDMWPOQ",
               "aws_secret_access_key": "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",
               "filebase": "tests/export_pipelines/",
               "predump_folder": "tmp"
           }
       },
       "persistence": {
           "name": "exporters.persistence.pickle_persistence.PicklePersistence",
           "options": {
             "file_path": "/tmp/"
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
##############
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