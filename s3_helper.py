"""
Module to handle S3 related tasks for the project.
1. Listing files from a bucket
2. Downloading the CSV files
"""
import codecs
import csv

import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from dataclasses import dataclass
import constants


@dataclass
class S3FileObject:
    """
    Dataclass for handling S3 File object
    """
    file_path: str
    last_modified_time: datetime
    file_size_in_bytes: int

    def __repr__(self):
        return self.file_path

    def __str__(self):
        return self.file_path

    def __lt__(self, other):
        return self.last_modified_time < other.last_modified_time


class S3Helper:
    """
    Class to handle S3 operations
    """
    def __init__(self, bucket_name, access_key, secret_key):
        """
        It creates S3 client and if the bucket doesn't exists, throws an error
        :param bucket_name: Name of the bucket to look into
        :param access_key: AWS Access Key
        :param secret_key: AWS Secret Key
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        if not self._bucket_exists():
            raise Exception(f"Bucket {self.bucket_name} does not exists.")

    def _bucket_exists(self):
        """
        Function to check if a bucket exists or not
        :return: True/False depends on the existence of the bucket
        """
        resp = self.s3_client.list_buckets()
        for bucket in resp["Buckets"]:
            if self.bucket_name in bucket["Name"]:
                return True
        return False

    def list_bucket(self, prefix="", last_modified_time=constants.MINIMUM_TIME, order_by_time=False):
        """
        Function to list bucket with given prefix and greater than last_modified_time
        :param prefix: Prefix to scan
        :param last_modified_time: to filter the files which are later then this date
        :param order_by_time: If true, the final list will be ordered according the last_modified_time,
                                by default it is False
        :return: List of S3FileObject
        """
        next_continuation_token = None
        new_files = []
        print(f"Looking in {self._full_path(prefix)}")
        while True:
            if next_continuation_token:
                resp = self.s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                      Prefix=prefix,
                                                      ContinuationToken=next_continuation_token,
                                                      )
            else:
                resp = self.s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                      Prefix=prefix,
                                                      )
            try:
                for obj in resp["Contents"]:
                    if obj["LastModified"] >= last_modified_time:
                        temp_file_object = S3FileObject(file_path=self._full_path(obj["Key"]),
                                                        last_modified_time=obj["LastModified"],
                                                        file_size_in_bytes=obj["Size"])
                        new_files.append(temp_file_object)

                next_continuation_token = resp["NextContinuationToken"]
            except KeyError:
                break

        if order_by_time:
            new_files.sort()

        return new_files

    def _full_path(self, key=""):
        """
        Function to prepare the full s3 path
        :param key: Key part of the path
        :return: full s3 path
        """
        full_path = f"s3://{self.bucket_name}"
        if key:
            full_path = f"{full_path}/{key}"

        return full_path

    @staticmethod
    def get_bucket_and_key(s3_url: str):
        """
        Function to separate bucket and key from the full s3_url
        :param s3_url: Full S3 URL
        :return: Bucket and Key
        """
        if not s3_url.startswith("s3://"):
            raise Exception(f"The url is not S3 path {s3_url}")

        return s3_url[5:].split("/", maxsplit=1)

    def read_csv(self, s3_url):
        """
        Function to read CSV file from the S3
        :param s3_url: full S3 URl of the file
        :return: data row
        """
        try:
            bucket, key = self.get_bucket_and_key(s3_url=s3_url)

            obj = self.s3_client.get_object(Bucket=bucket, Key=key)

            for row in csv.DictReader(codecs.getreader("utf-8")(obj["Body"])):
                yield row
        except ValueError as vrr:
            raise vrr
        except ClientError as cerr:
            raise cerr
        except Exception as err:
            raise err
