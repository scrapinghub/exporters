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