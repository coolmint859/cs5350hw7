import boto3
import sys
import json
import logging


def create_widget():
    pass


def delete_widget():
    pass


def update_widget():
    pass


def get_next_widget():
    pass


def main():
    session = boto3.Session()
    s3 = session.client('s3')

    bucket_info = s3.list_buckets()

    for bucket in bucket_info["Buckets"]:
        print(bucket["Name"])


if __name__ == "__main__":
    main()
