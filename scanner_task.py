"""
Module to handle the Scanner
"""
import yaml
from db_helper.etl_metadata_database import ETLMetadataDatabaseConnector, ScannerTable, ScannerStatusEnum
from s3_helper import S3Helper, S3FileObject
from config_data_classes import DatabaseConfig, S3Config, ETLConfig
import constants
from datetime import datetime as dt
from datetime import timedelta as td
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ScannerTask:
    """
    Class to handle all Scanner related tasks
    """
    def __init__(self, etl_db_config: DatabaseConfig, s3_config: S3Config, etl_config: ETLConfig):
        """
        Initialising connection to S3 and ETL Database
        :param etl_db_config: ETL database config object
        :param s3_config: S3 config Object
        :param etl_config: ETL tasks related object
        """
        self.etl_db = ETLMetadataDatabaseConnector(db_name=etl_db_config.DATABASE_NAME,
                                                   host=etl_db_config.HOST,
                                                   port=etl_db_config.PORT,
                                                   user=etl_db_config.USERNAME,
                                                   password=etl_db_config.PASSWORD)

        self.s3_helper = S3Helper(bucket_name=s3_config.BUCKET,
                                  access_key=s3_config.AWS_ACCESS_KEY,
                                  secret_key=s3_config.AWS_SECRET_KEY)

        self.etl_config = etl_config

    @staticmethod
    def _get_prefix(parts: list, path_sep="/"):
        """
        Function to generate path from list of parts
        :param parts: List of strings
        :param path_sep: Path separator for the final path
        :return: A path string
        """
        prefix = ""
        for index, part in enumerate(parts):
            if isinstance(part, int) and part <= 9:
                prefix += "0"
            prefix += str(part)

            if index != len(parts) - 1:
                prefix += path_sep

        return prefix

    def _generate_prefixes(self, from_time):
        """
        Generating all the Keys to scan in the S3 according to the from_time to current hour
        :param from_time: Time from the prefix is required to be generated
        :return: List of prefixes
        example:
        from_time = "2021-10-05 00:20:00"
        now = "2021-10-05 04:30:00"
        prefixes = ["2021/10/05/00/", "2021/10/05/01/", "2021/10/05/02/",
                    "2021/10/05/04/", "2021/10/05/05/"]
        """
        # 1. Today in UTC
        now_utc = dt.now(tz=constants.TZ)

        # 2. Generate possible prefixes by dates
        possible_prefixes = []
        latest_year, latest_month, latest_date, latest_hour, *_ = now_utc.timetuple()
        while True:
            year, month, date, hour, *_ = from_time.timetuple()
            prefix = self._get_prefix(parts=[year, month, date, hour])
            possible_prefixes.append(prefix)

            if (year == latest_year
                    and month == latest_month
                    and date == latest_date
                    and hour == latest_hour):
                break
            from_time = from_time + td(hours=1)

        return possible_prefixes

    def _create_new_jobs(self, list_of_new_file_obj: [S3FileObject]):
        """
        Function to create new jobs to insert into Scanner Table
        This job will have following arguments
            1. files: List of all file paths which are grouped into one task
            2. job_size: Sum of all the file sizes in bytes
            3. latest_last_modified_time: The latest last_modified_time in the group of files of a job
        :param list_of_new_file_obj: List of S3FileObjects
        :return: List of ScannerTable jobs
        """
        new_jobs = []  # Final object to contain all the new Scanner Jobs
        job_size = 0
        files_in_job = []
        latest_last_modified_time_in_s3 = constants.MINIMUM_TIME

        for index, scanned_file in enumerate(list_of_new_file_obj):
            if job_size < self.etl_config.JOB_SIZE_IN_BYTES:
                job_size += scanned_file.file_size_in_bytes
                files_in_job.append(scanned_file.file_path)
                latest_last_modified_time_in_s3 = scanned_file.last_modified_time
            else:
                new_jobs.append(ScannerTable(files=constants.MULTI_FILE_PATH_SEPARATOR.join(files_in_job),
                                             total_size_in_bytes=job_size,
                                             latest_file_modified_time=latest_last_modified_time_in_s3,
                                             status=ScannerStatusEnum.SENT_FOR_ETL,))
                job_size = scanned_file.file_size_in_bytes
                files_in_job = [scanned_file.file_path]
                latest_last_modified_time_in_s3 = scanned_file.last_modified_time

            if index == len(list_of_new_file_obj) - 1:
                new_jobs.append(ScannerTable(files=constants.MULTI_FILE_PATH_SEPARATOR.join(files_in_job),
                                             total_size_in_bytes=job_size,
                                             latest_file_modified_time=latest_last_modified_time_in_s3,
                                             status=ScannerStatusEnum.SENT_FOR_ETL, ))
        return new_jobs

    def run(self):
        """
        Function to run the whole Scanner pipeline
        :return: None
        """
        logging.log(logging.INFO, "Scanner started!")
        # 1. Fetch the latest modified time from the Scanner Task DB
        latest_last_modified_time_in_db = self.etl_db.get_scanner_latest_modified_time()

        # 2. Generate prefixes from last time to current time
        possible_prefixes = self._generate_prefixes(from_time=latest_last_modified_time_in_db)

        # 3. Pass it to the S3 bucket and fetch the latest files in increasing last_modified_time
        list_of_new_file_obj: [S3FileObject] = []
        for prefix in possible_prefixes:
            list_of_new_file_obj += self.s3_helper.list_bucket(prefix=prefix,
                                                               last_modified_time=latest_last_modified_time_in_db,
                                                               order_by_time=True)

        if list_of_new_file_obj:
            logging.log(logging.INFO, f"Total files from S3 scanned: {len(list_of_new_file_obj)}")

            # 4. Create new jobs
            new_jobs = self._create_new_jobs(list_of_new_file_obj)

            # 5. Insert new jobs into the table for ETL task
            self.etl_db.create_new_jobs(rows=new_jobs)

            logging.log(logging.INFO, f"Created {len(new_jobs)} new ETL jobs.")
        else:
            logging.log(logging.INFO, "No new files to scan!")


if __name__ == "__main__":
    # Importing all the configurations
    with open("config.yaml", "r") as conf_file:
        config = yaml.safe_load(conf_file)

    _etl_db_config = DatabaseConfig(**config["METADATA_DATABASE"])
    _s3_config = S3Config(**config["S3"])
    _etl_config = ETLConfig(**config["ETL"])

    ScannerTask(etl_db_config=_etl_db_config,
                s3_config=_s3_config,
                etl_config=_etl_config).run()
