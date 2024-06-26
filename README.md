# S3 File Management and Notification System

This project automates the organization and management of files within an AWS S3 bucket. It is designed to move files from the root of the S3 bucket into structured directories based on sales representative identifiers and notifies the respective sales representatives via AWS SNS when their files have been moved.

![Screenshot_1](https://github.com/Mohamed-kittany/S3-File-Management-and-Notification-System/assets/161580792/517e7348-16f3-4d87-9fdc-2137295d2c6b)


## Features

- **File Reorganization**: Moves files from the root directory of the S3 bucket to a structured directory format based on predefined identifiers (e.g., sales representative IDs).
- **Duplication Avoidance**: Checks and prevents duplication by not moving files that already exist in the target directory.
- **Notifications**: Sends notifications to sales representatives via SNS to inform them of file movements.
- **Logging**: Records operations and errors in a local log file and uploads this log to S3.
- **Scheduled Execution**: Automatically runs the file management tasks at scheduled intervals throughout the day.

## Usage

1. **Set up your environment variables**:
   - `BUCKET_NAME`: Name of the S3 bucket
   - `CSV_LOCAL_DIRECTORY`: Local directory where CSV files are stored before being uploaded
   - `S3_BUCKET_SOURCE_PREFIX_DIR`: The prefix under which files will be organized in the S3 bucket
   - `LOCAL_LOG_FILE_NAME`: Local log file name
   - `LOG_S3_KEY_NAME`: Key for the log file in the S3 bucket
   - `TOPIC_NAME`: Name of the SNS topic
   - `EMAIL_ADDRESS`: Email address to subscribe to the SNS topic

## Functions

- **`check_create_bucket`**: Ensures that the specified bucket exists or creates it if it does not.
- **`upload_files_to_s3`**: Uploads files from a local directory to an S3 bucket, organizing them by sales representative IDs.
- **`move_and_clean_s3_objects`**: Moves files within the S3 bucket into structured directories and cleans up any duplicates.
- **`upload_log_to_s3`**: Uploads the local log file to S3.
- **`create_sns_topic`, `get_sns_topic_arn`, `subscribe_email_to_topic`**: Manages SNS topics and subscribes emails for notifications.

## Automated Scheduling

To ensure that the system runs smoothly throughout the day without manual intervention, a scheduler is integrated into the system. It is set to execute the main function every 4 hours, which can be adjusted according to specific needs. This functionality is crucial for environments where regular updates are necessary.

- **Schedule Setup**: Utilizes Python's `schedule` library to run the main script at defined intervals.
- **Error Handling**: Includes robust error handling within the scheduling to ensure that any issues are logged and managed appropriately.
