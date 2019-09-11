# flake8: noqa
from setuptools import setup, find_packages

setup(
    name = 'exporters',
    version = '0.6.18',
    description = 'Exporters is an extensible export pipeline library that supports filter, '
                  'transform and several sources and destinations.',
    long_description = 'Exporters is an extensible export pipeline library that supports filter, '
                       'transform and several sources and destinations. They aim to provide a '
                       'flexible and easy to extend infrastructure for exporting data to and from multiple sources, '
                       'with support for filtering and transformation.',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords = 'exporters export data',
    url = 'https://github.com/scrapinghub/exporters',
    author = 'Scrapinghub',
    author_email = 'info@scrapinghub',
    license = 'BSD',
    packages = find_packages(exclude=['tests']),
    install_requires = ['six', 'retrying', 'requests', 'PyYAML', 'decorator'],
    dependency_links = [
        'git@github.com:scrapinghub/collection-scanner.git@0.1.5#egg=collection_scanner',
        'git@github.com:scrapinghub/flatson.git#egg=flatson',
        'git@github.com:scrapinghub/kafka-scanner.git@0.2.6#egg=kafka_scanner'
    ],
    extras_require = {
        'avro': ['fastavro'],
        'azure': ['azure'],
        'bz2': ['bz2file'],
        'csv': ['boltons'],
        'gcloud': ['gcloud'],
        'gdrive': ['PyDrive'],
        'hubstorage': ['hubstorage', 'collection_scanner'],
        'jq': ['jq'],
        'kafka': ['kafka-python', 'msgpack-python', 'kafka_scanner'],
        'mysql': ['mysql-python', 'SQLAlchemy'],
        'notifications': ['Jinja2'],
        'odo': ['flatson', 'odo', 'pandas'],
        'postgres': ['psycopg2', 'SQLAlchemy'],
        's3': ['boto', 'dateparser'],
        'sftp': ['pysftp', 'ecdsa', 'paramiko', 'pycrypto' 'wsgiref'],
        'sqlite': ['sqlitedict', 'SQLAlchemy'],
        'xml': ['dicttoxml'],
    },
)
