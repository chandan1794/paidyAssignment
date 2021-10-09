"""
Base class for creating database connector.
This class facilitates, all the required database operations.
"""

import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import constants
from datetime import datetime as dt


def now_with_timezone():
    """
    A wrapper function for dt.now with timezone, for table creations
    :return: a datetime.datetime object
    """
    return dt.now(tz=constants.TZ)


class DatabaseConnector:
    """
    Base Database Connector class.
    """
    def __init__(self, db_name, host, port, user, password):
        """
        Function to initiate the engine and session object.
        :param db_name: Name of the database to connect to
        :param host: Host address on which the database is
        :param port: Port for the database connection
        :param user: Username to access the database
        :param password: Password to access the database
        """
        self.engine = db.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
        self.Session = sessionmaker(self.engine)

    def setup_database(self):
        """
        Function to setup the database
        :return: None
        """
        pass

    def create_new_jobs(self, rows):
        """
        Function to implement multiple rows in a table of the database
        :param rows: List or Single Table object
        :return: None
        """
        if not isinstance(rows, list):
            rows = [rows]

        try:
            with self.Session.begin() as session:
                session.add_all(rows)
        except Exception as err:
            raise err

    @staticmethod
    def cast(sqlalchemy_type, value):
        """
        Function to cast values in sqlalchemy's Column's Python equivalent type
        :param sqlalchemy_type: Sqlalchemy Valid Column Types
        :param value: Value to cast
        :return: casted value
        """
        return sqlalchemy_type.python_type(value)
