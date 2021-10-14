"""
This is a script to setup
1. ETL Metadata Database
2. Reporting Database
   In the above two cases, if the table doesn't exists, this script will create the tables.
   The condition however is, the databases must exists before hand.
3. S3 config are correct or not.
   It checks it by trying to access the bucket in the config
"""

import yaml
from db_helper.etl_metadata_database import ETLMetadataDatabaseConnector
from db_helper.reporting_database import ReportingDatabaseConnector
from config_data_classes import DatabaseConfig, S3Config
from s3_helper import S3Helper


def database_setup(database_cls, database_config):
    """
    Function to setup the empty databases
    :param database_cls: Class of the database we are creating
    :param database_config: Database related configurations,
        database_name, username, password, host and port
    :return: None
    """
    dc = database_cls(db_name=database_config.DATABASE_NAME,
                      host=database_config.HOST,
                      port=database_config.PORT,
                      user=database_config.USERNAME,
                      password=database_config.PASSWORD)
    dc.setup_database()


def s3_check(s3_config):
    """
    Function to check if the bucket exists or not and the access
    and secret keys are correct or not
    :param s3_config: S3 config
    :return: None
    """
    S3Helper(bucket_name=s3_config.BUCKET,
             access_key=s3_config.AWS_ACCESS_KEY,
             secret_key=s3_config.AWS_SECRET_KEY)


if __name__ == "__main__":
    # Importing all the configurations
    with open("config.yaml", "r") as conf_file:
        config = yaml.safe_load(conf_file)

    # Check the database connections and existence
    database_setup(database_cls=ETLMetadataDatabaseConnector,
                   database_config=DatabaseConfig(**config["METADATA_DATABASE"]))

    # Check the Reporting Database
    database_setup(database_cls=ReportingDatabaseConnector,
                   database_config=DatabaseConfig(**config["REPORTING_DATABASE"]))

    # Check S3 config
    s3_check(S3Config(**config["S3"]))
