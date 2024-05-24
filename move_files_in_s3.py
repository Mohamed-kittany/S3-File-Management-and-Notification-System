#!/usr/bin/env python3
import boto3
import logging
from botocore.exceptions import ClientError
from utils import print_module_name


def move_and_clean_s3_objects(bucket_name, source_prefix, sns_topic_arn):
    """
    Moves and cleans files within an S3 bucket, reorganizing files from their original location to a structured
    directory based on sales representative identifiers. After moving, it notifies each sales representative via SNS.

    Args:
    - bucket_name (str): The name of the S3 bucket.
    - source_prefix (str): The directory prefix where files should be organized.
    - sns_topic_arn (str): The ARN of the SNS topic used for sending notifications.

    Returns:
    - tuple: A boolean indicating success or failure, and a message detailing the outcome of the operation.
    """
    print_module_name()
    print("Moving files within S3...")
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    sales_rep_files = {}

    try:
        print(f"Listing objects in bucket '{bucket_name}' without specific prefix...")
        # Requesting a list of all objects in the specified S3 bucket.
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in response:
            # Iterates through each object found in the bucket.
            for obj in response['Contents']:
                file_key = obj['Key']  # Storing the key of the object.
                # Checks if the current file is already located within the intended directory structure.
                if file_key.startswith(source_prefix):
                    continue

                # Extracts the sales representative (SR) identifier from the filename, assuming the filename is structured
                # such that the identifier precedes the first underscore (e.g., 'sr1_cust_01.csv').
                sr_identifier = file_key.split('_')[0]

                # Constructs a new path for the file to move it into the organized structure by appending the SR identifier.
                new_key = f"{source_prefix}{sr_identifier}/{file_key}"

                if not key_exists(s3_client, bucket_name, new_key):
                    print(f"Moving '{file_key}' to '{new_key}'...")
                    # Specifies the source file to be copied within the S3 bucket.
                    copy_source = {'Bucket': bucket_name, 'Key': file_key}
                    # Copies the object to a new location within the same bucket.
                    s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key)
                    # Deletes the original object from the bucket after successful copy.
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                    # Adds the file to a dictionary that tracks which files have been moved under each SR identifier.
                    sales_rep_files.setdefault(sr_identifier, []).append(file_key)
                    logging.info(f"Successfully moved {file_key} to {new_key}.")
                else:
                    print(f"'{new_key}' already exists, deleting the original file '{file_key}'...")
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                    logging.info(f"Deleted original file '{file_key}' as it already exists at '{new_key}'.")

        # Notify via SNS for each SR
        for sr, files in sales_rep_files.items():
            message = f"Files moved for SR{sr}: {', '.join(files)}"
            sns_client.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"Files Moved Notification for SR{sr}")
            logging.info(f"SNS notification sent for SR{sr}: {message}")

        if not sales_rep_files:
            print("No files found to move or delete.")
            return True, "No files were moved or deleted."

    except ClientError as e:
        error_msg = f"An AWS client error occurred: {e}"
        logging.error(error_msg)
        print(f"Error: {error_msg}")
        return False, error_msg

    return True, "Files successfully moved and notifications sent."


def key_exists(s3_client, bucket, key):
    """
    Checks if a specific key exists in the specified S3 bucket. This function is used to verify the presence of a file
    before attempting to move or delete it, ensuring that operations do not fail due to missing files.

    Args:
    - s3_client (boto3.client): The boto3 S3 client.
    - bucket (str): The name of the bucket where the file might exist.
    - key (str): The specific key of the file to check.

    Returns:
    - bool: True if the key exists, False if it does not. Raises an exception for other AWS errors.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(f"Failed to check existence of {key} due to {e}")
            raise e
