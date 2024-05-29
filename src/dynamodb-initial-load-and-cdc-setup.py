import boto3
import json
import logging
import argparse
import time
from datetime import datetime,timedelta
import pytz
from botocore.exceptions import ClientError

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

class DynamoDBInitialLoadAndCDC:
    '''
    Class to perform initial load and CDC setup for DynamoDB tables in cross-account environments
    '''
    def __init__(self):
        '''
        Constructor to initialize the class. Parse command line arguments and assigns them to instance variables.
        Parameters:
            None
        Returns:
            None
        '''
        # define command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--source-region", required=False, help="Region of the source DynamoDB table")
        parser.add_argument("--source-table-name", required=True, help="Source DynamoDB table name")
        parser.add_argument("--source-account-id", required=False, help="Source AWS account ID")
        parser.add_argument("--target-region", required=False, help="Region of the target DynamoDB table")
        parser.add_argument("--target-table-name", required=False, help="Target DynamoDB table name")
        parser.add_argument("--target-account-id", required=True, help="Target AWS account ID")
        parser.add_argument("--target-s3-bucket-name", required=False, help="Target S3 bucket name")
        parser.add_argument("--target-role-name", required=False, default="cross_account_assume_role", help="Target role name")
        parser.add_argument("--target-table-pk-name", required=False, help="Primary key name of the target DynamoDB table")
        parser.add_argument("--target-table-sk-name", required=False, help="Sort key name of the target DynamoDB table")
        parser.add_argument("--target-table-pk-type", required=False, help="Primary key type of the target DynamoDB table")
        parser.add_argument("--target-table-sk-type", required=False, help="Sort key type of the target DynamoDB table")
        parser.add_argument("--target-table-read-capacity", required=True, help="Read capacity of the target DynamoDB table")
        parser.add_argument("--target-table-write-capacity", required=True, help="Write capacity of the target DynamoDB table")
        parser.add_argument("--cdc-lambda-function-name", required=False, default="dynamodb-cross-account-cdc-lambda-function", help="Name of the CDC Lambda function")
        parser.add_argument("--lambda-event-source-batch-size", required=False, default=100, help="The maximum number of records in each batch that Lambda pulls from DynamoDB stream")

        # parse command line arguments and assign to variables
        args = parser.parse_args()

        if args.source_region is None:
            # get the current region if not provided
            session = boto3.session.Session()
            self.source_region = session.region_name
        else:
            self.source_region = args.source_region 

        # create a DynamoDB client object for the source region
        self.dynamodb = boto3.client('dynamodb', region_name=self.source_region)
        # create lambda client
        self.lambda_client = boto3.client('lambda', region_name=self.source_region)

        # check if source table exists
        self.source_table_name = args.source_table_name
        try:
            self.desc_src_tab_response = self.dynamodb.describe_table(TableName=self.source_table_name)
        except ClientError as e:
            logger.warning(f"Error: {e}")
            raise e

        # get the source account ID
        self.sts_client = boto3.client('sts')
        response = self.sts_client.get_caller_identity()
        source_account_id = response['Account']

        self.source_account_id = args.source_account_id if args.source_account_id is not None else source_account_id

        # if target region is not provided, use the source region
        self.target_region = args.target_region if args.target_region is not None else self.source_region

        # if target table name is not provided, use the source table name
        self.target_table_name = args.target_table_name if args.target_table_name is not None else self.source_table_name

        self.target_account_id = args.target_account_id
        self.target_s3_bucket_name = args.target_s3_bucket_name if args.target_table_name is not None else f"dynamodb-export-to-s3-{self.target_region}-{self.target_account_id}"

        # get the target role ARN
        target_role_name = args.target_role_name
        self.target_role_arn = f"arn:aws:iam::{self.target_account_id}:role/{target_role_name}"

        # if any of target table attribute is not provided, use the source table for the missing attributes
        self.target_table_pk_name = args.target_table_pk_name if args.target_table_pk_name is not None else self.desc_src_tab_response['Table']['KeySchema'][0]['AttributeName']
        self.target_table_sk_name = args.target_table_sk_name if args.target_table_sk_name is not None else self.desc_src_tab_response['Table']['KeySchema'][1]['AttributeName']
        self.target_table_pk_type = args.target_table_pk_type if args.target_table_pk_type is not None else self.desc_src_tab_response['Table']['AttributeDefinitions'][0]['AttributeType']
        self.target_table_sk_type = args.target_table_sk_type if args.target_table_sk_type is not None else self.desc_src_tab_response['Table']['AttributeDefinitions'][1]['AttributeType']
        
        self.target_table_read_capacity = int(args.target_table_read_capacity)
        self.target_table_write_capacity = int(args.target_table_write_capacity)
        self.cdc_lambda_function_name = args.cdc_lambda_function_name
        self.lambda_event_source_batch_size = int(args.lambda_event_source_batch_size)

        self.s3_prefix = None
        self.source_table_stream_arn = None
        self.dynamodb_stream_enabled_ts = None
        self.is_dynamodb_stream_enabled = False
        self.export_time = None
        self.cdc_lambda_config = None


    def check_if_PITR_enabled (self):
        '''
        Check if PITR is enabled on the source table. PITR is required to take export of the table
        Parameters:
            None
        Returns:
            is_pitr_enabled (bool): False if PITR is not enabled and True if enabled
        '''
        response = self.dynamodb.describe_continuous_backups(TableName=self.source_table_name)
        is_pitr_enabled = response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus'] == 'ENABLED'
        return is_pitr_enabled


    def enable_dynamodb_stream_on_source_table(self):
        '''
        Enable DynamoDB stream on the source table.
        Parameters:
            None
        Returns:
            None
        '''
        # check if DynamoDB streams is already enabled on the source table, if not then enable it  
        if ('StreamSpecification' not in self.desc_src_tab_response.get('Table')) or ( not self.desc_src_tab_response.get('Table').get('StreamSpecification','').get('StreamEnabled')):
            # enable DynamoDB stream on the source table
            response = self.dynamodb.update_table(
            TableName=self.source_table_name,
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
            }
            )

            # get the stream ARN
            self.source_table_stream_arn = response['TableDescription']['LatestStreamArn']

            timestamp_str = self.source_table_stream_arn.split('/')[-1]

            # Convert the timestamp string to a datetime object
            timestamp_without_ms = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%dT%H:%M:%S")
            timestamp = datetime.strptime(timestamp_without_ms, "%Y-%m-%dT%H:%M:%S")


            # Set the timezone to UTC
            timestamp = timestamp.replace(tzinfo=pytz.UTC)

            # Convert the datetime object to a Unix timestamp
            self.dynamodb_stream_enabled_ts = int(timestamp.timestamp())

            logger.info(f"Enabled DynamoDB stream on '{self.source_table_name}' and the stream ARN is '{self.source_table_stream_arn}'. The stream enabled at {self.dynamodb_stream_enabled_ts} epoch time")

        # If DynamoDB streams is already enabled then get the LatestStreamArn
        else:
            logger.info("DynamoDB stream is already enabled on the source table")
            self.is_dynamodb_stream_enabled = True
            self.source_table_stream_arn = self.desc_src_tab_response['Table']['LatestStreamArn']
    
    def export_dynamodb_table_to_s3(self):
        '''
        Export the source DynamoDB table to S3 bucket in the target account.
        Parameters:
            None
        Returns:
            None
        '''
        # get S3Prefix in YYYYMMDDHHMMSS format
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        self.s3_prefix = f"{current_time}-{self.source_table_name}-export"

        # create an export task to export the source DynamoDB table data to S3 bucket in target account
        logger.info(f"Exporting DynamoDB {self.source_table_name} table to S3 location s3://{self.target_s3_bucket_name}/{self.s3_prefix} " 
                    f"in the target account")
       
        # If the source table already has a DynamoDB stream enabled, export the table with the latest timestamp
        if self.is_dynamodb_stream_enabled:
            response = self.dynamodb.export_table_to_point_in_time(
                TableArn=f"arn:aws:dynamodb:{self.source_region}:{self.source_account_id}:table/{self.source_table_name}",
                S3Bucket= self.target_s3_bucket_name,
                S3BucketOwner=self.target_account_id,
                S3Prefix=self.s3_prefix,
                ExportFormat='DYNAMODB_JSON'
            )
        # If the migration script enables the DynamoDB stream, export the table with the timestamp corresponding to the stream activation.
        else:
            response = self.dynamodb.export_table_to_point_in_time(
                TableArn=f"arn:aws:dynamodb:{self.source_region}:{self.source_account_id}:table/{self.source_table_name}",
                ExportTime=self.dynamodb_stream_enabled_ts,
                S3Bucket= self.target_s3_bucket_name,
                S3BucketOwner=self.target_account_id,
                S3Prefix=self.s3_prefix,
                ExportFormat='DYNAMODB_JSON'
            )

        self.export_time = response['ExportDescription']['ExportTime']

        # round down export time to closest second
        self.export_time = self.export_time.replace(microsecond=0)

        logger.info(f"Export task created with point in time: {self.export_time}")

        # wait for the export task to complete by checking the status
        while True:
            response = self.dynamodb.describe_export(
                ExportArn=response['ExportDescription']['ExportArn']
            )
            if response['ExportDescription']['ExportStatus'] != 'IN_PROGRESS':
                break
            else:
                logger.info(f"Export task status is {response['ExportDescription']['ExportStatus']} , waiting for 30 seconds to complete")
                time.sleep(30)
            
        if response['ExportDescription']['ExportStatus'] == 'COMPLETED':
            logger.info(f"DynamoDB {self.source_table_name} table is exported to S3")
        else:
            logger.error(f"Export task failed with error {response['ExportDescription']['FailureCode']}")
            raise Exception(f"Export task failed with error {response['ExportDescription']['FailureCode']}")

    def assume_target_role(self):
        '''
        Assumes the target role in the target account and returns the assumed role credentials.
        Parameters:
            None
        Returns:
            dict: The assumed role credentials.
        '''
        # assume the target role in the target account
        response = self.sts_client.assume_role(
            RoleArn=self.target_role_arn,
            RoleSessionName='AssumeRoleSession'
        )
        return response['Credentials']

    def get_S3_key_prefix(self,target_role_credentials):
        '''
        Get the latest S3 key prefix from the target S3 bucket.
        Parameters:
            target_role_credentials (dict): The assumed role credentials.
        Returns:
            str: The latest S3 key prefix.
        '''
        s3 = boto3.client('s3', region_name=self.target_region,
                  aws_access_key_id=target_role_credentials['AccessKeyId'],
                  aws_secret_access_key=target_role_credentials['SecretAccessKey'],
                  aws_session_token=target_role_credentials['SessionToken'])

        response = s3.list_objects_v2(Bucket=self.target_s3_bucket_name,Prefix=self.s3_prefix)

        # get the latest S3 key prefix
        for obj in response['Contents']:
            if 'data/' in obj['Key']:
                S3KeyPrefix = obj['Key'].rsplit('/', 1)[0]+'/'
                break
        return S3KeyPrefix

    def import_data_from_s3_to_dynamodb(self):
        '''
        Import data from S3 to the target DynamoDB table in the target account. After the import completes, 
        establish a resource-based policy on the target table to grant write access to the CDC AWS Lambda function.
        
        Parameters:
            None
        Returns:
            None
        '''
       # assume target role
        target_role_credentials = self.assume_target_role()

        # get the latest S3 key prefix
        S3KeyPrefix = self.get_S3_key_prefix(target_role_credentials)

        # Create DynamoDB client with target role credentials
        dynamodb = boto3.client('dynamodb', region_name=self.target_region,
                                aws_access_key_id=target_role_credentials['AccessKeyId'],
                                aws_secret_access_key=target_role_credentials['SecretAccessKey'],
                                aws_session_token=target_role_credentials['SessionToken'])
        
        # import data from S3 to DynamoDB table in target account
        logger.info(f"Importing DynamoDB table {self.source_table_name} in target account from S3 location s3://{self.target_s3_bucket_name}/{S3KeyPrefix}")
        response = dynamodb.import_table(
            S3BucketSource = {
                'S3Bucket': self.target_s3_bucket_name,
                'S3KeyPrefix': S3KeyPrefix
            },
            InputFormat = 'DYNAMODB_JSON',
            InputCompressionType = 'GZIP',
            TableCreationParameters = {
                'TableName': self.target_table_name,
                'KeySchema': [
                    {
                        'AttributeName': self.target_table_pk_name,
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': self.target_table_sk_name,
                        'KeyType': 'RANGE'
                    }
                ],
                'AttributeDefinitions': [
                    {
                        'AttributeName': self.target_table_pk_name,
                        'AttributeType': self.target_table_pk_type
                    },
                    {
                        'AttributeName': self.target_table_sk_name,
                        'AttributeType': self.target_table_sk_type
                    }
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': self.target_table_read_capacity,
                    'WriteCapacityUnits': self.target_table_write_capacity
                }
            },

        )

        # wait for the import task to complete by checking the status 
        while True:
            try:
                response = dynamodb.describe_import(
                    ImportArn=response['ImportTableDescription']['ImportArn']
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ExpiredTokenException':
                    # renew the assumed role session
                    target_role_credentials = self.assume_target_role()
                    dynamodb = boto3.client('dynamodb', region_name=self.target_region,
                                            aws_access_key_id=target_role_credentials['AccessKeyId'],
                                            aws_secret_access_key=target_role_credentials['SecretAccessKey'],
                                            aws_session_token=target_role_credentials['SessionToken'])
                    response = dynamodb.describe_import(
                        ImportArn=response['ImportTableDescription']['ImportArn']
                    )
                else:
                    raise
            if response['ImportTableDescription']['ImportStatus'] != 'IN_PROGRESS':
                break
            else:
                logger.info(f"Import task status is {response['ImportTableDescription']['ImportStatus']} , waiting for 30 seconds to complete")
                time.sleep(30)

        if response['ImportTableDescription']['ImportStatus'] == 'COMPLETED':
            logger.info("Import is completed")
        else:
            logger.error(f"Import task failed with status {response['ImportTableDescription']['ImportStatus']}")
            raise Exception(f"Import task failed with status {response['ImportTableDescription']['ImportStatus']}")     

        # Create a resource-based policy on the target DynamoDB table for the CDC AWS Lambda function.
        self.cdc_lambda_config = self.lambda_client.get_function_configuration(FunctionName=self.cdc_lambda_function_name)
        policy_document = {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                            "Sid": "1",
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": self.cdc_lambda_config['Role']
                            },
                            "Action": [
                                "dynamodb:PutItem",
                                "dynamodb:DeleteItem"
                            ],
                            "Resource": response['ImportTableDescription']['TableArn']
                            }
                        ]
                    }
        policy_document_str = json.dumps(policy_document)
        dynamodb.put_resource_policy(
                    ResourceArn=response['ImportTableDescription']['TableArn'],
                    Policy= policy_document_str
                )             
              
    def create_ddb_stream_lambda_trigger(self):
        '''
        Enables the DynamoDB trigger to the AWS Lambda function for CDC
        Parameters:
            None
        Returns:
            None
        '''

        # If the source table already had a DynamoDB stream enabled, update the CDC lambda function environment variable with the export time
        # If the DynamoDB stream was enabled by the migration script, retain the default export time of '1900-01-01 00:00:00.00-0000'
        if self.is_dynamodb_stream_enabled:
            # Retrieve the current environment variables
            environment_variables = self.cdc_lambda_config['Environment']['Variables']
            # Update the environment variables to include EXPORT_TIME
            environment_variables.update( {'EXPORT_TIME': self.export_time.strftime('%Y-%m-%d %H:%M:%S%z')})

            # Update the EXPORT_TIME environment variables of CDC lambda function
            self.lambda_client.update_function_configuration(
                FunctionName= self.cdc_lambda_function_name,
                Environment={
                    'Variables': environment_variables
                }
            )

        # set the starting position of the stream based on the DynamoDB stream status
        starting_position = 'TRIM_HORIZON' if  self.is_dynamodb_stream_enabled else 'LATEST'

        # create a Lambda function trigger on the DynamoDB stream
        self.lambda_client.create_event_source_mapping(
            EventSourceArn=self.source_table_stream_arn,
            FunctionName=self.cdc_lambda_function_name,
            Enabled=True,
            BatchSize=self.lambda_event_source_batch_size,
            StartingPosition=starting_position
        )
        logger.info(f"Created a Lambda function trigger on the DynamoDB stream of {self.source_table_name}")


if __name__ == "__main__":
    # create DynamoDBInitialLoadAndCDC object
    ddb_initial_load_and_cdc = DynamoDBInitialLoadAndCDC()
    logger.info("Starting DynamoDB initial load and CDC setup")

    # check if PITR is enabled
    if not ddb_initial_load_and_cdc.check_if_PITR_enabled():
        raise ValueError("PITR is disabled on the source table. Enable PITR before starting the migration")
    
    # enable DynamoDB stream on the source table
    ddb_initial_load_and_cdc.enable_dynamodb_stream_on_source_table()

    # export DynamoDB table to S3 bucket in target account
    ddb_initial_load_and_cdc.export_dynamodb_table_to_s3()

    # import data from S3 to DynamoDB table in target account
    ddb_initial_load_and_cdc.import_data_from_s3_to_dynamodb()

    # Enable CDC on the DynamoDB table in source account
    ddb_initial_load_and_cdc.create_ddb_stream_lambda_trigger()

