# flake8: noqa
from setuptools import setup, find_packages

setup(
    name = 'exporters',
    version = '0.6.13',
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
        'bz2': ['bz2file'],
        'sftp': ['pysftp', 'ecdsa', 'paramiko', 'pycrypto' 'wsgiref'],
        's3': ['boto', 'dateparser'],
        'hubstorage': ['hubstorage', 'collection_scanner'],
        'jq': ['jq'],
        'odo': ['flatson', 'odo', 'pandas'],
        'kafka': ['kafka-python', 'msgpack-python', 'kafka_scanner'],
        'notifications': ['Jinja2'],
        'csv': ['boltons'],
        'gcloud': ['gcloud'],
        'gdrive': ['PyDrive'],
        'sqlite': ['sqlitedict', 'SQLAlchemy'],
        'postgres': ['psycopg2', 'SQLAlchemy'],
        'mysql': ['mysql-python', 'SQLAlchemy'],
        'azure': ['azure'],
        'xml': ['dicttoxml'],
    },
)
