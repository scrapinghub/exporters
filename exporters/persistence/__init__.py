from .pickle_persistence import PicklePersistence
from .alchemy_persistence import MysqlPersistence, PostgresqlPersistence

persistence_list = [PicklePersistence, MysqlPersistence, PostgresqlPersistence]