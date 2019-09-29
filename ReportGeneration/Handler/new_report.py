import datetime as dt
import logging
import time
from datetime import datetime
from io import StringIO

import boto3
import holidays
from boto3.dynamodb.conditions import Key

# import pandas as pd
from ReportGeneration.Accessors.AWSAccessors import AWSAccessors
from ReportGeneration.Errors.IncorrectNumberOfInputsError import IncorrectNumberOfInputs
from ReportGeneration.Models.Member import Member
from ReportGeneration.Models.Standby import Standby
from ReportGeneration.Utils.csv_utils import CSVUtils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def report_generation_handler(event, context):
    logger.info('Started report generation process')

    logger.info('Attempting to obtain S3 objects')
    bucket_name = 'new-bvfco11'
    s3_accessor = AWSAccessors('s3')
    s3_resource = s3_accessor.get_resource()
    s3_bucket = s3_resource.Bucket(bucket_name)
    logger.info('Successfully obtained S3 object')

    logger.info('Attempting to obtain DynamoDB objects')
    dynamo_db_accessor = AWSAccessors('dynamodb')
    dynamo_db_resource = dynamo_db_accessor.get_resource()
    dynamo_db_table = dynamo_db_resource.Table('TestMemberStatistics')
    logger.info('Successfully obtained DynamoDB object')

    try:
        keys = instantiate_keys(s3_bucket)
    except IncorrectNumberOfInputs:
        return 1

    csv = CSVUtils()

    all_reports = []

    # Get station standby csv from S3 bucket
    station_standby_report = csv.read_s3_object(s3_accessor.get_object_from_s3(bucket_name, keys["Station_Standby"]), 1)
    # all_reports.append(station_standby_report)

    # Get member list csv from S3 bucket
    member_list = csv.read_s3_object(s3_accessor.get_object_from_s3(bucket_name, keys["Member_List"]), 2)
    # all_reports.append(member_list)

    # Get ambulance response report csv from S3 bucket
    ambulance_response_report = csv.read_s3_object(
        s3_accessor.get_object_from_s3(bucket_name, keys["Ambulance_Report"]), 1)
    # all_reports.append(ambulance_response_report)

    # Get engine response report csv from S3 bucket
    engine_response_report = csv.read_s3_object(s3_accessor.get_object_from_s3(bucket_name, keys["Engine_Report"]), 1)
    # all_reports.append(engine_response_report)

    # Get chief response report csv from S3 bucket
    chief_response_report = csv.read_s3_object(s3_accessor.get_object_from_s3(bucket_name, keys['Chief_Report']), 1)
    # all_reports.append(chief_response_report)

    # Get scheduled report csv from S3 bucket
    scheduled_report = csv.read_s3_object(s3_accessor.get_object_from_s3(bucket_name, keys["Scheduled_Report"]), 1)
    # all_reports.append(scheduled_report)

    initialize_members(member_list, dynamo_db_table)

    return 0


def instantiate_keys(s3_bucket):
    # Prefixes for keys in the S3 bucket. This will ignore any date specific file names. Add all keys to a list for
    # deletion after the program has executed
    station_standby_prefix = 'Station_Standby_Record'
    member_list_prefix = 'Member_List'
    ambulance_report_prefix = 'Ambulance_Response_Report'
    engine_report_prefix = 'Engine_Response_Report'
    chief_report_prefix = 'Chief_Response_Report'
    scheduled_report_prefix = 'Scheduled_Time_Report'
    finished_report_prefix = 'finished'

    # Key instantiation for file retrieval from S3 bucket
    station_standby_key = ''
    member_list_key = ''
    ambulance_report_key = ''
    engine_report_key = ''
    chief_report_key = ''
    scheduled_report_key = ''
    finished_key = ''

    # Iterate through all objects in the S3 bucket, and set the appropriate key names to the variables
    # instantiated above
    for obj in s3_bucket.objects.all():
        key = obj.key
        if key.startswith(station_standby_prefix):
            station_standby_key = key
        elif key.startswith(member_list_prefix):
            member_list_key = key
        elif key.startswith(ambulance_report_prefix):
            ambulance_report_key = key
        elif key.startswith(engine_report_prefix):
            engine_report_key = key
        elif key.startswith(chief_report_prefix):
            chief_report_key = key
        elif key.startswith(scheduled_report_prefix):
            scheduled_report_key = key
        elif key.startswith(finished_report_prefix) and key is not None:
            finished_key = key
        elif key.startswith('lambda_package'):
            continue

    keys = {
        "Station_Standby": station_standby_key,
        "Member_List": member_list_key,
        "Ambulance_Report": ambulance_report_key,
        "Engine_Report": engine_report_key,
        "Chief_Report": chief_report_key,
        "Scheduled_Report": scheduled_report_key,
        "Finished": finished_key
    }

    for key in keys:
        if key == '' and key is not 'Finished':
            incorrect_key = key
            message = 'The following key was incorrect: {} Please try again after uploading all required files.' \
                .format(incorrect_key)
            logger.error(message)
            raise IncorrectNumberOfInputs(message)

    logger.info('Keys: {}'.format(keys))
    return keys


def initialize_members(member_list, dynamo_db_table):
    logger.info('Member List: {}'.format(member_list))
    for row in member_list:
        member_id = str(row['DeptID'])
        member_name = str(row['Member'])

        current_member = Member(member_name, member_id)

        logger.info('Member Name: {}'.format(current_member.get_name()))
        logger.info('Member Id: {}'.format(current_member.get_id()))

        count = dynamo_db_table.query(
            Select='COUNT',
            KeyConditionExpression=Key('ID_Number').eq(member_id)

        )

        if count['Count'] == 0:
            dynamo_db_table.put_item(
                Item={
                    'ID_Number': current_member.get_id(),
                    'Member Name': current_member.get_name(),
                    'Number Missed Duty Shift Months': '0',
                    'Warning Level': 'green'
                }
            )

    return 0


report_generation_handler(None, None)
