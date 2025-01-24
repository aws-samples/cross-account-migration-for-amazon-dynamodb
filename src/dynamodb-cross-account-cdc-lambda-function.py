import os
import json
import boto3
import base64
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from datetime import datetime,timezone

# Initialize Logger from Lambda Powertools
logger = Logger()

target_aws_account_num = os.environ['TARGET_AWS_ACCOUNT_NUMBER']
target_ddb_region = os.environ['TARGET_REGION']
target_ddb_name = os.environ['TARGET_DYNAMODB_TABLE_NAME']
sqs_queue_for_failed_events = os.environ['SQS_QUEUE_URL_FOR_FAILED_EVENTS']
export_time_str = os.environ['EXPORT_TIME']
target_table_arn = f"arn:aws:dynamodb:{target_ddb_region}:{target_aws_account_num}:table/{target_ddb_name}"

# Create DynamoDB client object
dynamodb = boto3.client('dynamodb', region_name=target_ddb_region)
# Create SQS client object
sqs = boto3.client('sqs')

def decode_bytes_if_present(items_by_type: dict) -> None:
    # Look for binary values, b64 decode them back into bytes
    for key, value_dict in items_by_type.items():
        for value_type, value in value_dict.items():
            if value_type == "B":
                items_by_type[key] = {value_type: base64.b64decode(value)}

# Lambda function handler
def lambda_handler(event, context):
    '''
    Lambda function handler to process DynamoDB CDC events and copy them to target DynamoDB table in different account.
    Failed events are sent to the SQS queue. Please note, there's currently no retry mechanism in place for these events; 
    however, retry functionality can be implemented as per the specific use case.
    '''
    total_records = len(event['Records'])
    failed_records = 0

    # convert export_time_str into epoch time
    export_datetime_obj = datetime.strptime(export_time_str, "%Y-%m-%d %H:%M:%S%z")
    export_datetime_obj_utc = export_datetime_obj.astimezone(timezone.utc)
    epoch_export_time = int(export_datetime_obj_utc.timestamp())

    try:
        # Process each record in the event
        Records = event['Records']
        for record in Records:
            # check if DynamoDB stream record was created after the export was taken
            if record['dynamodb']['ApproximateCreationDateTime'] >= epoch_export_time:
                event_name = record['eventName']
                try:
                    # Perform DynamoDB operations based on event type
                    if event_name == 'REMOVE':
                        response = dynamodb.delete_item(TableName=target_table_arn, Key=record['dynamodb']['Keys'])
                    else:
                        ddb_item_to_put = record['dynamodb']['NewImage']
                        decode_bytes_if_present(ddb_item_to_put)
                        response = dynamodb.put_item(TableName=target_table_arn, Item=ddb_item_to_put)
                except ClientError as e:
                    logger.exception("An error occurred: %s", e)
                    # add failed event to SQS queue with error message
                    failed_records += 1
                    add_failed_event_to_sqs(sqs_queue_for_failed_events, record, str(e))
            else:
                logger.info("Ignoring the DynamoDB stream record since it was created before the export was taken.")
        
        logger.info(f"Total {total_records} records to process")
        logger.info(f"Successfully processed {total_records-failed_records} records")
        if failed_records > 0:
            logger.info(f"Failed to process {failed_records} records")
        
    except ClientError as e:
        logger.warning("An error occurred: %s", e)
        raise e

# Function to add failed event to SQS queue
def add_failed_event_to_sqs(sqs_queue_for_failed_events, record, error_message):
    '''
    Function to add failed event to SQS queue.
    Parameters:
        sqs_queue_for_failed_events (str): The URL of the SQS queue.
        record (dict): The failed event record.
        error_message (str): The error message.
    Returns:
        None
    '''
    try:
        # Send the failed event to the SQS queue
        encryption_context = {
            "TableName": target_ddb_name,
            "Region": target_ddb_region,
            "OperationType": record['eventName'],
            "Timestamp": int(datetime.now().timestamp())
        }
        logger.info(f"Sending failed event to SQS queue: {record} with encryption_context {encryption_context}")           
        sqs.send_message(
            QueueUrl=sqs_queue_for_failed_events,
            MessageBody=json.dumps(record),
            MessageAttributes={
                'Error': {
                    'DataType': 'String',
                    'StringValue': error_message
                },
                'RecordType': {
                    'DataType': 'String',
                    'StringValue': record['eventName']
                },
                'EncryptionContext' : {
                    'DataType': 'String',
                    'StringValue': json.dumps(encryption_context)
                }
            }
        )
    except ClientError as e:
        logger.warning("An error occurred: %s", e)
        raise e

