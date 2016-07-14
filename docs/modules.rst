.. _modules:

Modules
=======
Every module has a `supported_options` attribute that defines which options are optional or mandatory,
and the default values if proceeds. It is a dict with the following shape:

.. code-block:: python
    supported_options = {
        'option_name': {[attributes]}
    }


Possible attributes for a supported_option are:

    - type - The option type. In addition to the standard python types (``basestring``, ``int``, ``list``, ``dict``...],
      exporters provide homogeneous list types in ``exporters.utils`` called ``str_list``, ``int_list`` and
      ``dict_list``, This types indicate that every member of the list needs to be able to be be casted
      to a string, integer or dictionary respectively.
    - default - Default option value if it is not provided by configuration object. If it is present,
    the supported_option will be optional instead of mandatory.
    - env_fallback - If option is not provided by configuration object, it will be loaded from
    env_fallback environment variable.


Export Manager
~~~~~~~~~~~~~~
This module handles the pipeline iteration.
A pipeline iteration usually consists on calling the reader to get a batch, filter it, transform it,
filter it again, write it and commit the read batch. It should also be in charge of notifications
and retries management.

Provided exporters
******************

BasicExporter
#############
.. automodule:: exporters.export_managers.basic_exporter
    :members:
    :undoc-members:
    :show-inheritance:


Bypass support
~~~~~~~~~~~~~~
Exporters architecture provides support for bypassing the pipeline. One example would be in which both reader
and writer aim S3 buckets. If no transforms or filtering are needed, keys can be copied directly without downloading them.

All bypass classes are subclasses of BaseBypass class, and must implement two methods:

    - meets_conditions(configuration)
            Checks if provided export configuration meets the requirements to use the bypass. If not, it returns False.

    - execute()
        Executes the bypass script.

    - close()
        Perform all needed actions to leave a clean system after the bypass execution.

Provided Bypass scripts
***********************
S3Bypass
########
.. automodule:: exporters.bypasses.s3_to_s3_bypass
    :members:
    :undoc-members:
    :show-inheritance:

S3ToAzureBlobBypass
###################
.. automodule:: exporters.bypasses.s3_to_azure_blob_bypass
    :members:
    :undoc-members:
    :show-inheritance:

S3ToAzureFileBypass
###################
.. automodule:: exporters.bypasses.s3_to_azure_file_bypass
    :members:
    :undoc-members:
    :show-inheritance:

StreamBypass
############
.. automodule:: exporters.bypasses.stream_bypass
    :members:
    :undoc-members:
    :show-inheritance:


Reader
~~~~~~
There are two kinds of readers, stream readers and structured readers. Stream readers read a stream of
data from a binary backend, while in structured readers the underlying storage stores structured items.

Stream readers have to be combined with a decompressor and a deserializer to read the items from them (by
default, ZLibDecompressor and JsonDeserializer are used).

Readers are in charge of providing batches of items to the pipeline. All readers are subclasses of
BaseReader class.

 Structured readers must implement:
    - get_next_batch()
        This method is called from the manager. It must return a list or a generator of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.

 Stream readers must implement:
    - get_read_streams()
        This should be an iterator that yields streams (file-like objects).


.. automodule:: exporters.readers.base_reader
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: exporters.readers.base_stream_reader
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

FSReader (Stream)
#################
.. automodule:: exporters.readers.fs_reader
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

S3Reader (Stream)
#################
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
Writers are in charge of writing batches of items to final destination. All writers are subclasses of
BaseWriter class, and must implement:

    - write(dump_path, group_key=None)
        The manager calls this method. It consumes a dump_path, which is the path of an
        items buffer file compressed with gzip. It also has an optional group_key, which provides
        information regarding the group membership of the items contained in that file.


All writers have also the following common options:

    - items_per_buffer_write
        Number of items to be written before a buffer flush takes place.

    - size_per_buffer_write
        Size of buffer files before being flushed.

    - items_limit
        Number of items to be written before ending the export process. This is useful for
        testing exports.


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

SFTPWriter
##########
.. automodule:: exporters.writers.sftp_writer
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


GStorageWriter
##############
.. automodule:: exporters.writers.gstorage_writer
    :members:
    :undoc-members:
    :show-inheritance:


HubstorageReduceWriter
######################
.. automodule:: exporters.writers.hs_reduce_writer
    :members:
    :undoc-members:
    :show-inheritance:


ReduceWriter
############
.. automodule:: exporters.writers.reduce_writer
    :members:
    :undoc-members:
    :show-inheritance:


OdoWriter
#########
.. automodule:: exporters.writers.odo_writer
    :members:
    :undoc-members:
    :show-inheritance:

HubstorageWriter
################
.. automodule:: exporters.writers.hubstorage_writer
    :members:
    :undoc-members:
    :show-inheritance:



Transform
~~~~~~~~~
You can apply some item transformations as a part of an export job. Using this module, read items can
be modified or cleaned before being written. To add a new transform module, you must overwrite the following method: 

    - transform_batch(batch)
         Receives the batch, transforms its items and yields them,

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

PythonmapTransform
##################
.. automodule:: exporters.transform.pythonmap
    :members:
    :undoc-members:
    :show-inheritance:

Filter
~~~~~~
This module receives a batch, filters it according to some parameters, and returns it.
It must implement the following method:

- filter(item)
    It receives an item and returns True if the item must be included, or False otherwise

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
This module is in charge of resuming support. It persists the current
state of the read and written items, and inform of that state on demand. It's usually called
from an export manager, and must implement the following methods:

    - get_last_position()
        Returns the last commited position

    - commit_position(last_position)
        Commits a position that has been through all the pipeline. Position can be any serializable object. This supports both
        usual position abstractions (number of batch) or specific abstractions such as offsets in Kafka (which are dicts)

    - generate_new_job()
        Creates and instantiates all that is needed to keep persistence (tmp files, remote connections...)

    - close()
        Cleans tmp files, closes remote connections...

    - configuration_from_uri(uri, regex)
        returns a configuration object

It must also define a `uri_regex` to help the module find a previously created resume abstraction.

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
You can define notifications for main export job events such as starting an export, ending or failing.
These events can be sent to multiple destinations by adding proper modules to an export configuration.


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

WebhookNotifier
###############
.. automodule:: exporters.notifications.webhook_notifier
    :members:
    :undoc-members:
    :show-inheritance:


Grouping
~~~~~~~~
This module adds support for grouping items. It must implement the following methods:

    - group_batch(batch)
        It adds grouping info to all the items from a batch. Every item, which is a BaseRecord,
        has a group_membership attribute that should be updated by this method before yielding it

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

NoGrouper
#########
.. automodule:: exporters.groupers.no_grouper
    :members:
    :undoc-members:
    :show-inheritance:

PythonExpGrouper
################
.. automodule:: exporters.groupers.python_exp_grouper
    :members:
    :undoc-members:
    :show-inheritance:


Stats Managers
~~~~~~~~~~~~~~
This module provides support for keeping track of export statistics. A Stats Manager must implement
the following methods:

    - iteration_report(times, stats)
        It recieves the times spent in every step of the export pipeline iteration, and
        the aggregated stats

    - final_report(stats)
        Usually called at the end of an export job


.. automodule:: exporters.stats_managers.base_stats_managers
    :members:
    :undoc-members:
    :show-inheritance:


Provided Stats Managers
***********************

BasicStatsManager
#################
.. automodule:: exporters.stats_managers.basic_stats_manager
    :members:
    :undoc-members:
    :show-inheritance:

LoggingStatsManager
###################
.. automodule:: exporters.stats_managers.logging_stats_manager
    :members:
    :undoc-members:
    :show-inheritance:


Export Formatters
~~~~~~~~~~~~~~~~~
Exporters use formatter modules to export in different formats. An export formatter must implement the following method:

    - format(batch)
        It adds formatting info to all the items from a batch. Every item, which is a BaseRecord,
        has a formatted attribute that should be updated by this method before yielding it

.. automodule:: exporters.export_formatters.base_export_formatter
    :members:
    :undoc-members:
    :show-inheritance:


Provided Export Formatters
**************************

JsonExportFormatter
###################
.. automodule:: exporters.export_formatter.json_export_formatter
    :members:
    :undoc-members:
    :show-inheritance:

CSVExportFormatter
##################
.. automodule:: exporters.export_formatter.csv_export_formatter
    :members:
    :undoc-members:
    :show-inheritance:

XMLExportFormatter
##################
.. automodule:: exporters.export_formatter.xml_export_formatter
    :members:
    :undoc-members:
    :show-inheritance:


Decompressors
~~~~~~~~~~~~~
Decompressors take a compressed stream from a stream reader and return an uncompressed stream.
They have to implement one function:

    - decompress(stream)
        Decompress the input stream (returns an uncompressed stream)

.. automodule:: exporters.decompressors.base_decompressor
    :members:
    :undoc-members:
    :show-inheritance:


Provided Decompressors
**********************

ZLibDecompressor
################
.. automodule:: exporters.decompressors.zlib_decompressor
    :members:
    :undoc-members:
    :show-inheritance:

NoDecompressor
##############
.. automodule:: exporters.decompressors.no_decompressor
    :members:
    :undoc-members:
    :show-inheritance:


Deserializers
~~~~~~~~~~~~~
Deserializers take a stream of uncompressed bytes and returns an iterator of records
They have to implement one function:

    - deserialize(stream)
        Deserialize the input stream (return an iterator of records)

.. automodule:: exporters.deserializers.base_deserializer
    :members:
    :undoc-members:
    :show-inheritance:


Provided deserializers
**********************

JsonDeserializer
################
.. automodule:: exporters.deserializers.json_deserializer
    :members:
    :undoc-members:
    :show-inheritance:

CSVDeserializer
###############
.. automodule:: exporters.deserializers.csv_deserializer
    :members:
    :undoc-members:
    :show-inheritance:
