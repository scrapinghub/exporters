.. _modules:

Modules
=======


Export Manager
~~~~~~~~~~~~~~
This module is in charge of the pipeline iteration, and it is the one executed to start it.
A pipeline iteration usually consists on calling the reader to get a batch, filter it, transform it,
filter it again, write it and commit the read batch. It should also be  in charge of notifications
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
Exporters arqchitecture provides support to bypass the pipeline. A usage example of that is the case in which both reader
and writer aim S3 buckets. If no transforms or filtering are needed, keys can be copied directly without downloading them.

All bypass classes are subclasses of BaseBypass class, and must implement two methods:

    - meets_conditions(configuration)
            Checks if provided export configuration meets the requirements to use the bypass. If not, a RequisitesNotMet
            exception must be thrown.

    - bypass()
        Executes the bypass script.

    - close()
        Perform all needed actions to leave a clean system after the bypass execution.

Provided Bypass scripts
***********************
S3Bypass
########
.. automodule:: exporters.export_managers.s3_to_s3_bypass
    :members:
    :undoc-members:
    :show-inheritance:

Reader
~~~~~~
Readers are in charge of providing batches of items to the pipeline. All readers are subclasses of
 BaseReader class, and must implement:

    - get_next_batch()
        This method is called from the manager. It must return a list or a generator of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.


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

FSReader
############
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
Writers are in charge of writing batches of items to final destination. All writers are subclasses of
BaseWriter class, and must implement:

    - write(dump_path, group_key=None)
        This method is called from the manager. It gets a dump_path, which is the path of an
        items buffer file compressed with gzip. It also has an optional group_key, which provides
        information regarding the group membership of the items contained in that file.


All writers have also the following common options:

    - items_per_buffer_write
        Number of items before a buffer flush takes place.

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



Transform
~~~~~~~~~
Transformations to items can be made in an export job. Using this modules, read items can
be modified or cleaned before being written. To add new transform modules, this methods must
be overwritten:

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

Filter
~~~~~~
This module receives a batch, filter it according to some parameters, and returns it.
It must implement the following methods:

- filter(item)
    It receives an item and returns True if the filter must be included, or False if not

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
This module is in charge of resuming support. It must be able to persist the current
state of read and written items, and inform of that state on demand. It is usually called
from an export manager, and it must implement the following methods:

    - get_last_position()
        Returns the last commited position

    - commit_position(last_position)
        Commits a position that has been through all the pipeline. Position can be any serializable object. This support both
        usual position abstractions (number of batch) of specific abstractions such as offsets in Kafka (which are a dict)

    - generate_new_job()
        Creates and instantiates all that is needed to keep persistence (tmp files, remote connections...)

    - close()
        Cleans tmp files, close remote connections...

    - configuration_from_uri(uri, regex)
        returns a configuration object

It must also define a `uri_regex`, which is a regex that must help the module to find
an already created resume abstraction

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
Exporters project supports notification of main export job events, like starting an export, ending or failing.
This events can be notified to multiple destinations by adding proper modules to an export configuration.


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
This module adds support to grouping items. It must implement the following methods:

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
This module provides support for keeping track of export stats. A Stats Manager must implement
the following methods:

    - iteration_report(times, stats)
        It recieves the spent times in every step of the export pipeline iteration, and
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
Exporters project support exporting to different formats. This formatting is handled by
export formatters modules. An export formatter must implement the following method:

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