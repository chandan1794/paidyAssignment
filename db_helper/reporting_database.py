"""
This file defines a class to handle database operations of Reporting database
"""
from sqlalchemy import Integer, Column, Float
from sqlalchemy.ext.declarative import declarative_base

from .database_connector import DatabaseConnector


Base = declarative_base()


class LoanApplicationsTable(Base):
    """
    Table definition of Loan Application class
    """
    __tablename__ = "loan_applications"
    id = Column(Integer(), primary_key=True, autoincrement=True)
    serious_dlqin_2_yrs = Column(Integer())
    revolving_utilization_of_unsecured_lines = Column(Float())
    age = Column(Integer())
    number_of_time_30_59_days_past_due_not_worse = Column(Integer())
    debt_ratio = Column(Float())
    monthly_income = Column(Integer())
    number_of_open_credit_lines_and_loans = Column(Integer())
    number_of_time_90_days_late = Column(Integer())
    number_real_estate_loans_or_lines = Column(Integer())
    number_of_times_60_89_days_past_due_not_worse = Column(Integer())
    number_of_dependents = Column(Integer)


class ReportingDatabaseConnector(DatabaseConnector):
    """
    Class to handle Reporting database related operations
    """
    def __init__(self, db_name, host, port, user, password):
        super().__init__(db_name, host, port, user, password)

    def setup_database(self):
        # We can safely call this multiple times, it won't affect the schema or the data
        # in the tables.
        # If in case, we want to change the schema, first we need to migrate the data for that
        Base.metadata.create_all(self.engine)
