"""
This file defines a class to handle database operations of ETL Metadata database
"""

import enum

from sqlalchemy import Integer, Column, VARCHAR, TIMESTAMP, Enum
from sqlalchemy import func as sqlalchemy_func
from sqlalchemy.ext.declarative import declarative_base

from .database_connector import DatabaseConnector, now_with_timezone

import constants

Base = declarative_base()


class ScannerStatusEnum(enum.Enum):
    """
    Class to set the valid status for the Scanner Table
    """
    NONE = "NONE"
    SENT_FOR_ETL = "SENT_FOR_ETL"
    PROCESSING = "PROCESSING"
    LOADED = "LOADED"
    FAILED = "FAILED"


class ScannerTable(Base):
    """
    Table definition of Scanner Metadata class
    """
    __tablename__ = "scanner_metadata"
    id = Column(Integer(), primary_key=True, autoincrement=True)
    files = Column(VARCHAR(4096), nullable=False)
    latest_file_modified_time = Column(TIMESTAMP(), nullable=False)
    total_size_in_bytes = Column(Integer(), nullable=False)
    created_time = Column(TIMESTAMP(), default=now_with_timezone)
    modified_time = Column(TIMESTAMP(), default=now_with_timezone, onupdate=now_with_timezone)
    status = Column(Enum(ScannerStatusEnum), nullable=False, default=ScannerStatusEnum.SENT_FOR_ETL.value)
    failure_msg = Column(VARCHAR(10240), nullable=True, default="")

    def __str__(self):
        return f"JobId: {self.id}, Files: {self.files}, SizeInBytes:{self.total_size_in_bytes}, Status:{self.status}"

    def __repr__(self):
        return f"JobId: {self.id}, Files: {self.files}, SizeInBytes:{self.total_size_in_bytes}, Status:{self.status}"


class ETLMetadataDatabaseConnector(DatabaseConnector):
    """
    Class to handle ETL Metadata database related operations
    """
    def __init__(self, db_name, host, port, user, password):
        super().__init__(db_name, host, port, user, password)

    def setup_database(self):
        # We can safely call this multiple times, it won't affect the schema or the data
        # in the tables.
        # If in case, we want to change the schema, first we need to migrate the data for that
        Base.metadata.create_all(self.engine)

        # The first job is required to be added to the Scanner Task table, for the scanner to
        # pick up the latest last_modified_time
        with self.Session.begin() as session:
            # If the table in newly built, then only insert the dummy row
            if not session.query(ScannerTable).count():
                dummy_scanner_row = ScannerTable(
                    files="DUMMY_FILE",
                    latest_file_modified_time=constants.MINIMUM_TIME,
                    total_size_in_bytes=0,
                    status=ScannerStatusEnum.LOADED,
                )
                session.add(dummy_scanner_row)

    def get_scanner_latest_modified_time(self):
        """
        Function to fetch the latest file_modified_time from the table.
        This time indicated at what time we have scanned the last file for processing.
        :return: A datetime object
        """
        with self.Session.begin() as session:
            latest_last_modified_time = (
                session
                .query(sqlalchemy_func.max(ScannerTable.latest_file_modified_time))
                .scalar()
            )
            return latest_last_modified_time.replace(tzinfo=constants.TZ)

    def _change_status_of_job(self, session, job_id, new_status: ScannerStatusEnum, **kwargs):
        """
        Function to prepare a query to change the job status.
        :param session: Session in which the query will be executed
        :param job_id: Jon Id for which the status is required to be changed
        :param new_status: The new status to which the status has to be updated
        :param kwargs: Extra arguments, if new_status is FAILED, then a error msg is passed in it
        :return: None
        """
        update_dict = {ScannerTable.status: new_status.value}
        if new_status == ScannerStatusEnum.FAILED:
            update_dict[ScannerTable.failure_msg] = kwargs["err_msg"]

        (
            session
            .query(ScannerTable)
            .filter(ScannerTable.id == job_id)
            .update(update_dict)
        )

    def get_latest_etl_job(self):
        """
        Returns the latest job with the status SENT_FOR_ETL
        :return: ScannerTable row
        """
        with self.Session.begin() as session:
            # Fetching the latest job where status is SENT_FOR_ETL
            latest_job = (
                session
                .query(ScannerTable)
                .where(ScannerTable.status == ScannerStatusEnum.SENT_FOR_ETL.value)
                .order_by(ScannerTable.latest_file_modified_time)
                .first()
            )
            if latest_job is not None:
                # Updating its status to PROCESSING
                self._change_status_of_job(session=session,
                                           job_id=latest_job.id,
                                           new_status=ScannerStatusEnum.PROCESSING)

                # Once the session expires the object can expire
                # https://stackoverflow.com/questions/15397680/detaching-sqlalchemy-instance-so-no-refresh-happens
                session.expunge(latest_job)
            return latest_job

    def mark_downloading_from_s3_failed(self, job_id, err_msg):
        """
        Function to mark an ETL job failed
        :param job_id: Job Id to be mark failed
        :param err_msg: error msg to insert in the job
        :return: None
        """
        with self.Session.begin() as session:
            self._change_status_of_job(session=session,
                                       job_id=job_id,
                                       new_status=ScannerStatusEnum.FAILED,
                                       err_msg=err_msg)

    def mark_downloading_from_s3_success(self, job_id):
        """
        Function to mark an ETL job successful
        :param job_id: Job Id to mark successful
        :return: None
        """
        with self.Session.begin() as session:
            self._change_status_of_job(session=session,
                                       job_id=job_id,
                                       new_status=ScannerStatusEnum.LOADED)
