#!/usr/bin/env python3
import boto3
import os
import logging
from botocore.exceptions import ClientError
from utils import print_module_name

def file_exists_in_s3(s3_client, bucket, key):
    """
    Checks if a specific file (key) exists in the specified S3 bucket. This function retrieves
    the file's metadata to determine existence. It returns False if the file does not exist,
    catching a 404 error. Other exceptions are propagated upwards.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e  # Propagate other exceptions

def upload_files_to_s3(bucket, csv_local_directory, source_prefix):
    """
    Uploads CSV files from a specified local directory to an S3 bucket under a structured
    directory prefix based on sales representative identifiers extracted from the filename.
    It ensures files are not duplicated by checking their existence before uploading.

    Args:
    - bucket (str): The name of the S3 bucket where files will be uploaded.
    - csv_local_directory (str): The local directory path where CSV files are stored.
    - source_prefix (str): The prefix under which files will be organized in the S3 bucket.

    Returns:
    - tuple: A boolean indicating whether the upload process was successful, and a message detailing the outcome.
    """
    print_module_name()
    print("Uploading files to S3...")

    s3_client = boto3.client('s3')
    files_uploaded = []
    error_messages = []

    for filename in os.listdir(csv_local_directory):
        if filename.endswith(".csv"):  # Process only CSV files.
            file_path = os.path.join(csv_local_directory, filename)
            sr_identifier = filename.split('_')[0]  # Extracting SR identifier from filename
            s3_key = f"{source_prefix}{sr_identifier}/{filename}"  # Formulating S3 key

            if not file_exists_in_s3(s3_client, bucket, s3_key):
                print(f"Preparing to upload {filename} to S3 with key {s3_key}...")
                try:
                    s3_client.upload_file(file_path, bucket, s3_key)
                    logging.info(f"Uploaded {filename} to S3 bucket {bucket} with key {s3_key}.")
                    files_uploaded.append(filename)
                    print(f"Successfully uploaded {filename} to S3.")
                except ClientError as e:
                    error_msg = f"Failed to upload {filename}. Error: {e}"
                    logging.error(error_msg)
                    error_messages.append(error_msg)
                    print(error_msg)
            else:
                print(f"{filename} already exists in S3 under '{s3_key}', skipping upload.")
                logging.info(f"{filename} already exists in S3 under '{s3_key}', skipping upload.")

    if files_uploaded:
        return True, f"Uploaded files: {', '.join(files_uploaded)}"
    else:
        return True, f"No need to upload. Files already exist."

