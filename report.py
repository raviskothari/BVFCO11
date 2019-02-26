from datetime import datetime
import boto3
import pandas as pd
from io import StringIO

# Lambda function that will run each time each time a PUT request is done in the 'bvfco11' S3 bucket. This function
# will calculate member statistics, including number of ambulance runs, number of engine runs, hours volunteered, etc.
def my_lambda_handler(event, context):
    # S3 credentials
    S3_BUCKET = 'bvfco11'
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Prefixes for keys in the S3 bucket. This will ignore any date specific file names
    station_standby_prefix = 'Station_Standby_Record'
    member_list_prefix = 'Member_List'
    ambulance_report_prefix = 'Ambulance_Response_Report'
    engine_report_prefix = 'Engine_Response_Report'

    # Key instantiation for file retrieval from S3 bucket
    station_standby_key = ''
    member_list_key = ''
    ambulance_report_key = ''
    engine_report_key = ''

    # Iterate through all objects in the S3 bucket, and set the appropriate key names to the variables
    # instantiated above
    for obj in s3.list_objects(Bucket=S3_BUCKET)['Contents']:
        key = obj['Key']
        if key.startswith(station_standby_prefix):
            station_standby_key = key
        elif key.startswith(member_list_prefix):
            member_list_key = key
        elif key.startswith(ambulance_report_prefix):
            ambulance_report_key = key
        elif key.startswith(engine_report_prefix):
            engine_report_key = key

    # Get station standby csv from S3 bucket
    station_standby_obj = s3.get_object(Bucket=S3_BUCKET, Key=station_standby_key)
    station_standby_report = pd.read_csv(station_standby_obj['Body'], header=1)

    # Get member list csv from S3 bucket
    member_list_obj = s3.get_object(Bucket=S3_BUCKET, Key=member_list_key)
    member_list = pd.read_csv(member_list_obj['Body'], header=2)

    # Get ambulance response report csv from S3 bucket
    ambulance_report_obj = s3.get_object(Bucket=S3_BUCKET, Key=ambulance_report_key)
    ambulance_response_report = pd.read_csv(ambulance_report_obj['Body'], header=1)

    # Get engine response report csv from S3 bucket
    engine_report_obj = s3.get_object(Bucket=S3_BUCKET, Key=engine_report_key)
    engine_response_report = pd.read_csv(engine_report_obj['Body'], header=1)

    # Initialize the dictionaries to map members to their appropriate statistics
    member_hours_dict = {}
    member_incentive_due_dict = {}
    member_ambulance_calls_dict = {}
    member_engine_calls_dict = {}

    # Iterate through member list and initialize all members with 0 stats
    for index, row in member_list.iterrows():
        if row.loc['Member'] not in member_hours_dict:
            member_hours_dict[row.loc['Member']] = 0
        if row.loc['Member'] not in member_incentive_due_dict:
            member_incentive_due_dict[row.loc['Member']] = 0
        if row.loc['Member'] not in member_ambulance_calls_dict:
            member_ambulance_calls_dict[row.loc['Member']] = 0
        if row.loc['Member'] not in member_engine_calls_dict:
            member_engine_calls_dict[row.loc['Member']] = 0
            
    # Iterate through station standby report and calculate total hours volunteered by each member
    for index2, row in station_standby_report.iterrows():
        member_hours_dict[row.loc['Submitted By']] += row.loc['Total Number of Hours']

    # Iterate through ambulance response report and calculate total ambulance incentive and
    # ambulance calls taken each member
    for index3, row in ambulance_response_report.iterrows():
        if row.loc['Transport'] == 'No':
            member_incentive_due_dict[row.loc['Driver']] += 5
            member_incentive_due_dict[row.loc['Aide/OIC']] += 5
        elif row.loc['Transport'] == 'Yes':
            member_incentive_due_dict[row.loc['Driver']] += 10
            member_incentive_due_dict[row.loc['Aide/OIC']] += 10
        else:
            member_incentive_due_dict[row.loc['Driver']] += 5
            member_incentive_due_dict[row.loc['Aide/OIC']] += 5

        member_ambulance_calls_dict[row.loc['Driver']] += 1
        member_ambulance_calls_dict[row.loc['Aide/OIC']] += 1
        if not (pd.isnull(row.loc['3rd'])):
            member_ambulance_calls_dict[row.loc['3rd']] += 1
        if not (pd.isnull(row.loc['Additional Crew'])):
            member_ambulance_calls_dict[row.loc['Additional Crew']] += 1

    # Iterate through engine response report and calculate total engine incentive and engine calls taken each member
    for index4, row in engine_response_report.iterrows():
        member_incentive_due_dict[row.loc['Driver']] += 5
        member_incentive_due_dict[row.loc['Officer-In-Charge']] += 5

        member_engine_calls_dict[row.loc['Driver']] += 1
        member_engine_calls_dict[row.loc['Officer-In-Charge']] += 1
        if not (pd.isnull(row.loc['Line'])):
            member_engine_calls_dict[row.loc['Line']] += 1
        if not (pd.isnull(row.loc['Backup'])):
            member_engine_calls_dict[row.loc['Backup']] += 1
        if not (pd.isnull(row.loc['Bars'])):
            member_engine_calls_dict[row.loc['Bars']] += 1
        if not (pd.isnull(row.loc['Layout'])):
            member_engine_calls_dict[row.loc['Layout']] += 1
        if not (pd.isnull(row.loc['Observer'])):
            member_engine_calls_dict[row.loc['Observer']] += 1

    # Initialize member lists and stats for conversion into Python DataFrame
    member_names = []
    member_hours = []
    member_incentive = []
    member_ambulance_calls = []
    member_engine_calls = []

    # Add all members and their associated stats into the appropriate list
    for member in member_hours_dict:
        member_names.append(member)
        member_hours.append(member_hours_dict[member])
        member_incentive.append(member_incentive_due_dict[member])
        member_ambulance_calls.append(member_ambulance_calls_dict[member])
        member_engine_calls.append(member_engine_calls_dict[member])

    # Set dictionary for final CSV report labels
    member_hour_final_dict = {
        'Member': member_names,
        'Station Standby Hours Reported': member_hours,
        'Incentive Due': member_incentive,
        'Ambulance Calls Taken': member_ambulance_calls,
        'Engine Calls Taken': member_engine_calls
    }

    # Get current month for naming the final CSV report
    currentMonth = datetime.now().strftime('%B')

    # Set the report title and initialize a buffer for conversion into a CSV
    report_upload = str(currentMonth) + '_Station_11_Report.csv'
    csv_buffer = StringIO()

    # Convert the data from lists into a Python DataFrame
    member_hours_dataframe = pd.DataFrame(member_hour_final_dict)

    # Convert the Python DataFrame into a CSV using Pandas
    member_hours_dataframe.to_csv(csv_buffer, index=False)

    # Put request for final CSV report into S3 bucket
    s3_resource.Object(S3_BUCKET, report_upload).put(Body=csv_buffer.getvalue())

    # Return success
    return 0

