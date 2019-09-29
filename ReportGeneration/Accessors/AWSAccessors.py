import logging

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class AWSAccessors:

    def __init__(self, aws_service):
        self.client = boto3.client(aws_service)
        self.resource = boto3.resource(aws_service)

    def get_client(self):
        return self.client

    def get_resource(self):
        return self.resource

    def get_dynamo_db_table(self, table_name):
        self.resource.Table(table_name)

    def get_s3_bucket(self, bucket_name):
        return self.resource.Bucket(bucket_name)

    def get_object_from_s3(self, bucket_name, key):
        obj = self.get_s3_bucket(bucket_name).Object(key)
        logger.info('Obj: {}'.format(obj))
        return obj
        # return pd.read_csv(obj['Body'], header=header_number)