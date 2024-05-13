#!/usr/bin/env python3
import boto3
import logging
from botocore.exceptions import ClientError
from utils import print_module_name


def check_create_bucket(bucket_name):
    """
       Checks if an S3 bucket exists and creates it if it does not. It handles the creation based on the AWS region
       of the session to ensure compliance with S3's bucket location requirements.

       Args:
       - bucket_name (str): The name of the S3 bucket to check or create.

       Returns:
       - tuple: A boolean indicating success or failure, and a message or error detail.
    """
    print_module_name()
    print("Checking or creating S3 bucket...")

    s3_client = boto3.client('s3')
    region_name = boto3.session.Session().region_name  # Dynamically fetch the region

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} already exists.")
        print(f"Check complete: Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' does not exist. Attempting to create it...")
            return create_bucket(s3_client, bucket_name, region_name)
        logging.error(f"Error accessing bucket {bucket_name}: {e}")
        print(f"Error accessing bucket '{bucket_name}': {e}")
        return False, str(e)

    return True, None


def create_bucket(s3_client, bucket_name, region_name):
    """
        Helper function to create an S3 bucket in a specified region. If the region is 'us-east-1', it creates
        the bucket without a location constraint; otherwise, it sets the appropriate location constraint.

        Args:
        - s3_client (boto3.client): The boto3 S3 client object used to interact with Amazon S3.
        - bucket_name (str): The name of the bucket to create.
        - region_name (str): The AWS region where the bucket will be created.

        Returns:
        - tuple: A boolean indicating success or failure, and a message or error detail.
    """
    try:

        if region_name == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully in 'us-east-1' (no location constraint needed).")
        else:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': region_name
            })
            print(f"Bucket '{bucket_name}' created successfully in region '{region_name}'.")
        logging.info(f"Bucket {bucket_name} created in region {region_name}.")
        return True, None
    except ClientError as e:
        logging.error(f"Failed to create bucket {bucket_name}: {e}")
        print(f"Failed to create bucket '{bucket_name}': {e}")
        return False, str(e)
