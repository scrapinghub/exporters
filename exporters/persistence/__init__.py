PERSISTENCE_LIST = []

try:
    from .alchemy_persistence import MysqlPersistence
    PERSISTENCE_LIST.append(MysqlPersistence)
except ImportError:
    pass

try:
    from .alchemy_persistence import PostgresqlPersistence
    PERSISTENCE_LIST.append(PostgresqlPersistence)
except ImportError:
    pass


from .pickle_persistence import PicklePersistence
PERSISTENCE_LIST.append(PicklePersistence)
from exporters.persistence.exporter_api_persistence import ExporterApiPersistence
PERSISTENCE_LIST.append(ExporterApiPersistence)

