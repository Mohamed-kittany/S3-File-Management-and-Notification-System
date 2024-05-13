#!/usr/bin/env python3
import boto3
import os
import logging
from botocore.exceptions import ClientError
from utils import print_module_name


def file_exists_in_s3(s3_client, bucket, key):
    """
    Checks if a specific file (key) exists in the specified S3 bucket. This is achieved by attempting to retrieve
    the file's metadata. If the file does not exist, a 404 error is caught and False is returned. Other exceptions
    are propagated up to the caller to handle them appropriately.

    Args:
    - s3_client (boto3.client): The boto3 S3 client.
    - bucket (str): The name of the S3 bucket.
    - key (str): The specific key of the file to check.

    Returns:
    - bool: True if the file exists, False if not. Raises an exception for other AWS errors.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e  # Propagate other exceptions.


def upload_files_to_s3(bucket, csv_local_directory):
    """
    Uploads CSV files from a specified local directory directly to the root of an S3 bucket. It checks for the
    existence of each file before uploading to avoid duplications.

    Args:
    - bucket (str): The name of the S3 bucket where files will be uploaded.
    - csv_local_directory (str): The local directory path where CSV files are stored.

    Returns:
    - tuple: A boolean indicating whether the upload process was successful, and a message detailing the outcome.
    """
    print_module_name()
    print("Uploading files to S3...")

    s3_client = boto3.client('s3')
    files_uploaded = []
    error_messages = []

    # List every file in the local directory
    for filename in os.listdir(csv_local_directory):
        if filename.endswith(".csv"):  # Process only CSV files type.
            file_path = os.path.join(csv_local_directory, filename)
            if not file_exists_in_s3(s3_client, bucket, filename):
                print(f"Preparing to upload {filename} to S3 with key {filename}...")
                try:
                    s3_client.upload_file(file_path, bucket, filename)
                    logging.info(f"Uploaded {filename} to S3 bucket {bucket} with key {filename}.")
                    files_uploaded.append(filename)
                    print(f"Successfully uploaded {filename} to S3.")
                except ClientError as e:
                    error_msg = f"Failed to upload {filename}. Error: {e}"
                    logging.error(error_msg)
                    error_messages.append(error_msg)
                    print(error_msg)
            else:
                print(f"{filename} already exists in S3, skipping upload.")
                logging.info(f"{filename} already exists in S3, skipping upload.")

    # Check if any files were uploaded successfully
    if files_uploaded:
        return True, f"Uploaded files: {', '.join(files_uploaded)}"
    else:
        return True, f"No need to upload. Files already exist."

