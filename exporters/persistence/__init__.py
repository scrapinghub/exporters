from .pickle_persistence import PicklePersistence

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


PERSISTENCE_LIST.append(PicklePersistence)
