.. _tutorials:

Exporters Tutorial
==================

In this tutorial, we are going to learn how the new exporters work. The purpose of this project is to have a tool to
allow us to export from a wide range of sources to a wide range of targets, allowing us to perform complex filtering and transformations to items.


Let's make a simple export
~~~~~~~~~~~~~~~~~~~~~~~~~~
With this tutorial, we are going to read a topic in a `Kafka <http://kafka.apache.org>`_ cluster into an S3 bucket, filtering out non-American comapnies. We need to create a proper configuration file. To do it we create a plain JSON file with the proper data.


Config file
***********
The configuration file should look like this:

.. code-block:: javascript

    {
        "exporter_options": {
            "log_level": "DEBUG",
            "logger_name": "export-pipeline",
            "formatter": {
                "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                "options": {}
            },
            "notifications":[
            ]
        },
        "reader": {
            "name": "exporters.readers.kafka_scanner_reader.KafkaScannerReader",
            "options": {
                "brokers": [LIST OF BROKERS URLS],
                "topic": "your-topic-name",
                "group": "exporters-test"
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
                "bucket": "your-bucket",
                "filebase": "tests/export_tutorial_{:%d-%b-%Y}",
                "aws_access_key_id": "YOUR-ACCESS-KEY",
                "aws_secret_access_key": "YOUR-ACCESS-SECRET"
            }
        }
    }


Save this file as ~/config.json, and you can launch the export job with the following command:

.. code-block:: python

    python bin/export.py --config ~/config.json





GDrive export tutorial
~~~~~~~~~~~~~~~~~~~~~~
Step by Step GDrvie export
**************************

First two steps will only have to be done once:

  1. Make sure you have your client-secret.json file. If not, follow the steps described `here <https://developers.google.com/drive/web/quickstart/python>`_.
     More info about this file can be found `here <https://developers.google.com/api-client-library/python/guide/aaa_client_secrets>`_.

  2. Get your credentials file. We have added a script that helps you with this process. Usage:

     .. code-block:: shell

        python bin/get_gdrive_credentials.py PATH_TO_CLIENT_SECRET_FILE

     It will open a tab in your browser where you can login with your Google account. When you
     do that, the script will print where the credentials file has been created.

     Now for every delivery:

  3. Ask the destination owner to create a folder and share it with your Google user.

  4. The folder will appear under `Shared with me <https://drive.google.com/drive/shared-with-me>`_ section.

     .. image:: _images/shared.png
        :scale: 60 %
        :alt: Shared with me screen
        :align: center

     Go there, right click on the shared folder and click on "Add to my drive".
     This will add the folder the client shared with you in your `My Drive
     <https://drive.google.com/drive/my-drive>`_. section, which can be seen by exporters.

     .. image:: _images/add_to.png
        :scale: 60 %
        :alt: Add to screen
        :align: center

  5. Configure writer filepath to point the client's folder. For example, if client shared
     with you a folder called "export-data", and you have added to your drive,
     writer configuration could look like:

     .. code-block:: python

        "writer":{
            "name": "exporters.writers.gdrive_writer.GDriveWriter",
            "options": {
                "filebase": "export-data/gwriter-test_",
                "client_secret": {client-secret.json OBJECT},
                "credentials": {credentials OBJECT}
            }
        }


  6. To run the export, you could use the bin/export.py:

     .. code-block:: python

        python export.py --config CONFIGPATH


Resume export tutorial
~~~~~~~~~~~~~~~~~~~~~~

Let's assume we have a failed export job, that was using this configuration:

.. code-block:: javascript

    {
        "reader": {
            "name": "exporters.readers.random_reader.RandomReader",
            "options": {
            }
        },
        "writer": {
            "name": "exporters.writers.console_writer.ConsoleWriter",
            "options": {

            }
        },
        "persistence":{
            "name": "exporters.persistence.pickle_persistence.PicklePersistence",
            "options": {
                "file_path": "job_state.pickle"
            }
        }
    }


To resume the export, you must run:

     .. code-block:: python

        python export.py --resume pickle://job_state.pickle
