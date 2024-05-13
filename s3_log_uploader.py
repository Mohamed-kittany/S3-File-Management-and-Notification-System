#!/usr/bin/env python3
import boto3
import logging
from botocore.exceptions import ClientError
from utils import print_module_name


def upload_log_to_s3(bucket, key, local_log_file):
    """
        Uploads a log file to an AWS S3 bucket. If an existing log file is found in the bucket, it appends the new log data
        to it. If no log exists, it starts a new log file in the S3 bucket. The function handles all interactions with the S3
        service, including reading, combining, and uploading log data, and provides feedback through prints and logs.

        Args:
        - bucket (str): The name of the S3 bucket where the log file will be uploaded.
        - key (str): The key under which the log file will be stored in the S3 bucket.
        - local_log_file (str): The path to the local log file that contains the new log data to be uploaded.

        Returns:
        - tuple: A boolean indicating success or failure of the upload, and a message detailing the error if one occurred.
    """
    print_module_name()
    print("Uploading the log file to S3...")
    s3_client = boto3.client('s3')
    try:
        # Attempt to retrieve the existing log file from S3
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            existing_log_data = response['Body'].read().decode('utf-8')
            print("Existing log data retrieved from S3.")
        except s3_client.exceptions.NoSuchKey:
            existing_log_data = ""
            print("No existing log found in S3. Starting a new log file.")

        # Read new log data from the local file
        with open(local_log_file, 'r') as file:
            new_log_data = file.read()
            print("New log data read from the local file.")

        # Combine the existing and new log data
        combined_log_data = existing_log_data + new_log_data
        print("Log data combined successfully.")

        # Upload the combined log data back to S3
        s3_client.put_object(Bucket=bucket, Key=key, Body=combined_log_data.encode('utf-8'))
        logging.info("Successfully uploaded the log file to S3.")
        print("Log file successfully uploaded to S3.")
        return True, None

    except ClientError as e:
        logging.error(f"Client error during log upload to S3: {e}")
        print(f"Client error: {e}")
        return False, str(e)
    except Exception as e:
        logging.error(f"Failed to upload the log file to S3: {e}")
        print(f"Unexpected error: {e}")
        return False, str(e)
