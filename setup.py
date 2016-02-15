from os.path import dirname, join
from setuptools import setup, find_packages

with open(join(dirname(__file__), 'exporters/VERSION'), 'rb') as f:
    version = f.read().decode('ascii').strip()

setup(
    name = 'exporters',
    version = version,
    packages = find_packages(exclude=['tests']),
    install_requires = ['six', 'retrying', 'requests', 'PyYAML'],
    dependency_links = [
        'git@github.com:scrapinghub/collection-scanner.git#egg=collection_scanner',
        'git@github.com:scrapinghub/flatson.git#egg=flatson',
        'git@github.com:scrapinghub/kafka-scanner.git#egg=kafka_scanner'
    ],
    extras_require = {
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
    },
)
