"""
Module defining the base task class
"""
from config_data_classes import DatabaseConfig, S3Config, ETLConfig
from db_helper import ETLMetadataDatabaseConnector, ReportingDatabaseConnector
from s3_helper import S3Helper


class BaseTask:
    """
    Base Task class for all the tasks
    """
    def __init__(self,
                 etl_db_config: DatabaseConfig = None,
                 reporting_db_config: DatabaseConfig = None,
                 s3_config: S3Config = None,
                 etl_config: ETLConfig = None):
        """
        Initialising connection to S3 and ETL Database
        :param etl_db_config: ETL database config object
        :param reporting_db_config: Reporting database config object
        :param s3_config: S3 config Object
        :param etl_config: ETL tasks related object
        """
        if etl_db_config is not None:
            self.etl_db = ETLMetadataDatabaseConnector(db_name=etl_db_config.DATABASE_NAME,
                                                       host=etl_db_config.HOST,
                                                       port=etl_db_config.PORT,
                                                       user=etl_db_config.USERNAME,
                                                       password=etl_db_config.PASSWORD)
        else:
            self.etl_db = None

        if reporting_db_config is not None:
            self.reporting_db = ReportingDatabaseConnector(db_name=reporting_db_config.DATABASE_NAME,
                                                           host=reporting_db_config.HOST,
                                                           port=reporting_db_config.PORT,
                                                           user=reporting_db_config.USERNAME,
                                                           password=reporting_db_config.PASSWORD)
        else:
            self.reporting_db = None

        if s3_config is not None:
            self.s3_helper = S3Helper(bucket_name=s3_config.BUCKET,
                                      access_key=s3_config.AWS_ACCESS_KEY,
                                      secret_key=s3_config.AWS_SECRET_KEY)
        else:
            self.s3_helper = None

        self.etl_config = etl_config

    def run(self):
        """
        Main function for all the tasks to implement
        :return:
        """
        pass
