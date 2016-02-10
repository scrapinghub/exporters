# Workaround
# http://bugs.python.org/issue15881
try:
    import multiprocessing
except:
    pass

from setuptools import setup

setup(
    name = 'exporters',
    version = '0.1',
    packages = ['exporters'],
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
        'alchemy_persistence': ['sqlitedict', 'SQLAlchemy', 'psycopg2', 'mysql-python'],
    },
)
