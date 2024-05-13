#!/usr/bin/env python3
import boto3
import logging
from utils import print_module_name


def create_sns_topic(sns_client, topic_name):
    """
    Creates an Amazon SNS topic with the specified name using the provided SNS client. It logs and prints the outcome
    of the operation, returning the status of creation and the topic's ARN or an error message if the creation fails.

    Args:
    - sns_client (boto3.client): The boto3 SNS client used to interact with Amazon SNS.
    - topic_name (str): The name of the SNS topic to create.

    Returns:
    - tuple: (bool, str) True and the topic ARN if successful, or False and an error message if not.
    """
    try:
        print(f"Creating SNS topic: {topic_name}")
        response = sns_client.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        logging.info(f"SNS Topic created with ARN: {topic_arn}")
        print(f"SNS Topic created successfully with ARN: {topic_arn}")
        return True, topic_arn
    except Exception as e:
        logging.error(f"Failed to create SNS Topic: {e}")
        print(f"Failed to create SNS Topic: {e}")
        return False, str(e)


def get_sns_topic_arn(topic_name):
    """
        Retrieves the Amazon Resource Name (ARN) of an SNS topic by its name. If the topic does not exist, it creates
        a new one. This function sets up an SNS client, checks existing topics, and either returns the ARN of an existing
        topic or proceeds to create a new one.

        Args:
        - topic_name (str): The name of the SNS topic to retrieve or create.

        Returns:
        - tuple: (bool, str) True and the topic ARN if found or successfully created, or False and an error message if not.
    """
    print_module_name()
    print("Setting up SNS Client...")
    sns_client = boto3.client('sns')  # Create an SNS client.
    try:
        print(f"Retrieving SNS topic ARN for: {topic_name}")
        response = sns_client.list_topics()
        for topic in response['Topics']:
            if topic_name in topic['TopicArn']:
                logging.info(f"SNS Topic already exists: {topic['TopicArn']}")
                print(f"SNS Topic already exists: {topic['TopicArn']}")
                return True, topic['TopicArn']
        # Create the topic if not found
        print(f"SNS Topic not found, creating new topic: {topic_name}")
        return create_sns_topic(sns_client, topic_name)
    except Exception as e:
        logging.error(f"Failed to retrieve or create SNS Topic: {e}")
        print(f"Failed to retrieve or create SNS Topic: {e}")
        return False, str(e)


def subscribe_email_to_topic(topic_arn, email_address):
    """
    Subscribes an email address to an SNS topic, checking first if the email is already subscribed. It handles
    the subscription process and ensures no duplicate subscriptions are made. If the email is not already subscribed,
    it initiates a new subscription.

    Args:
    - topic_arn (str): The ARN of the SNS topic to which the email will be subscribed.
    - email_address (str): The email address to subscribe.

    Returns:
    - tuple: (bool, Optional[str]) True and None if successful, True and 'Pending confirmation' if confirmation is pending,
      or False and an error message if an error occurs.
    """

    print(f"Subscribing email: {email_address} to topic...")
    sns_client = boto3.client('sns')  # Create an SNS client.
    try:
        print(f"Checking existing subscriptions for email: {email_address}")
        subscriptions = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        for subscription in subscriptions['Subscriptions']:
            if subscription['Protocol'] == 'email' and subscription['Endpoint'] == email_address:
                if subscription['SubscriptionArn'].endswith('PendingConfirmation'):
                    print(f"Subscription for {email_address} is still pending confirmation.")
                    return True, 'Pending confirmation'
                else:
                    print(f"Email address {email_address} is already subscribed.")
                    return True, None
        print(f"Subscribing {email_address} to SNS topic.")
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email_address
        )
        subscription_id = response.get('SubscriptionArn', 'Pending confirmation')
        print(f"Subscription initiated for {email_address}, status pending confirmation. Subscription ID: {subscription_id}")
        return True, None
    except Exception as e:
        logging.error(f"Failed to subscribe email: {e}")
        print(f"Failed to subscribe email: {e}")
        return False, str(e)
