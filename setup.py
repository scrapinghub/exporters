# Workaround
# http://bugs.python.org/issue15881
try:
    import multiprocessing
except:
    pass

from setuptools import setup, find_packages

setup(
    name             = 'exporters',
    version          = '0.1',
    packages         = find_packages(exclude=['tests']),
    test_suite       = 'tests',
    tests_require    = ['mock', 'moto', 'coverage'],
    install_requires = ['six', 'retrying', 'requests', 'wheel', 'decorator', 'PyYAML'],
    extras_require   = {
        'sftp': ['pysftp', 'ecdsa', 'paramiko', 'pycrypto' 'wsgiref'],
        's3': ['boto', 'dateparser'],
        'hubstorage': ['hubstorage', '-e git+git@github.com:scrapinghub/collection-scanner.git#egg=collection_scanne'],
        'jq': ['jq'],
        'odo': ['-e git+git@github.com:scrapinghub/flatson.git#egg=flatson', 'odo', 'pandas'],
        'kafka': ['kafka-python', 'msgpack-python', '-e git+git@github.com:scrapinghub/kafka-scanner.git#egg=kafka_scanner'],
        'notifications': ['Jinja2'],
        'csv': ['boltons'],
        'gcloud': ['gcloud'],
        'gdrive': ['PyDrive'],
        'alchemy_persistence': ['sqlitedict', 'SQLAlchemy', 'psycopg2', 'mysql-python'],
    },
)
