from exporters.persistence.base_sqlalchemy_persistence import BaseAlchemyPersistence


class MysqlPersistence(BaseAlchemyPersistence):
    """
    Manages persistence using a mysql database as a backend. It will add a row for every job in a table called Jobs.

    Needed parameters:

        - user (str)
            Username with access to mysql database

        - password (str)
            Password string

        - host (str)
            MySql host ip

        - port (int)
            MySql port

        - database (str)
            Name of the database in which store jobs persistence
    """
    PROTOCOL = 'mysql'
    # mysql://username:password@host:port/database/job_id
    uri_regex = 'mysql:\/\/(.+):(.+)@(.+):(\d+)\/(.+)\/(\d+)'


class PostgresqlPersistence(BaseAlchemyPersistence):
    """
    Manages persistence using a postgresql database as a backend. It will add a row for every job in a table called Jobs.

    Needed parameters:

        - user (str)
            Username with access to mysql database

        - password (str)
            Password string

        - host (str)
            Postgresql host ip

        - port (int)
            Postgresql port

        - database (str)
            Name of the database in which store jobs persistence
    """
    PROTOCOL = 'postgresql'
    # postgresql://username:password@host:port/database/job_id
    uri_regex = 'postgresql:\/\/(.+):(.+)@(.+):(\d+)\/(.+)\/(\d+)'