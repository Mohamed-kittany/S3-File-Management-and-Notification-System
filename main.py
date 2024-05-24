#!/usr/bin/env python3
from s3_bucket_manager import check_create_bucket
from s3_file_uploader import upload_files_to_s3
from move_files_in_s3 import move_and_clean_s3_objects
from s3_log_uploader import upload_log_to_s3
from sns_manager import get_sns_topic_arn, subscribe_email_to_topic
from utils import print_module_name, setup_logging, log_and_print_error_message


# ### Constants ### I kept the constants in the main file for simplicity.
CSV_LOCAL_DIRECTORY = './dct-sales-local'  # Local directory where CSV files are stored.
BUCKET_NAME = 'dct-sales-mohamed-kittany'  # S3 bucket name.
S3_BUCKET_SOURCE_PREFIX_DIR = 'dct-sales/'  # Prefix in S3 where CSV files are stored.
LOCAL_LOG_FILE_NAME = 'local_application_logs.log'  # Local log file name.
LOG_S3_KEY_NAME = f'{S3_BUCKET_SOURCE_PREFIX_DIR}application_logs.log'  # Log file key in S3.


# SNS Configuration
TOPIC_NAME = 'EmailSalesRepFromS3Bucket'
EMAIL_ADDRESS = 'kittanyenterprise@gmail.com'


def main():
    """Main function to manage S3 file operations and logging."""
    print_module_name()
    setup_logging(LOCAL_LOG_FILE_NAME)

    success, sns_topic_arn_or_error = get_sns_topic_arn(TOPIC_NAME)
    if not success:
        log_and_print_error_message(
            f"Failed to retrieve or create the SNS topic ARN: {sns_topic_arn_or_error}. Exiting...")
        return  # Stop execution of the script if we don't get/create the SNS topic ARN.
    else:
        print(f"SNS TOPIC: ${sns_topic_arn_or_error}")

    success, message = subscribe_email_to_topic(sns_topic_arn_or_error, EMAIL_ADDRESS)
    if not success:
        log_and_print_error_message(f"Failed to subscribe email: {message}. Exiting...")
        return  # Stop execution of the script if we can't subscribe to an email.

    success, message = check_create_bucket(BUCKET_NAME)
    if not success:
        log_and_print_error_message(f"Failed to check/create bucket: {message}. Exiting...")
        return  # Stop execution of the script if we failed to create/check for a s3 bucket.

    success, message = upload_files_to_s3(BUCKET_NAME, CSV_LOCAL_DIRECTORY, S3_BUCKET_SOURCE_PREFIX_DIR)
    if not success:
        log_and_print_error_message(f"Failed to upload files: {message}. Exiting...")
        return  # Stop execution of the script if uploading files to s3 has failed.

    success, message = move_and_clean_s3_objects(BUCKET_NAME, S3_BUCKET_SOURCE_PREFIX_DIR, sns_topic_arn_or_error)
    if not success:
        log_and_print_error_message(f"Failed to move files: {message}. Exiting...")
        return  # Stop execution of the script if moving files within s3 has failed.

    success, message = upload_log_to_s3(BUCKET_NAME, LOG_S3_KEY_NAME, LOCAL_LOG_FILE_NAME)
    if not success:
        log_and_print_error_message(f"Failed to upload log file: {message}. Exiting...")
        return  # Stop execution of the script if uploading log file to s3 has failed. # although this is the last line of the script :)


if __name__ == "__main__":
    main()
