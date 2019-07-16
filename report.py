import datetime
import time
import boto3
import pandas as pd
from io import StringIO


# Lambda function that will run each time each time a PUT request is done in the 'bvfco11' S3 bucket. This function
# will calculate member statistics, including number of ambulance runs, number of engine runs, hours volunteered, etc.
def my_lambda_handler(event, context):
    # S3 credentials
    s3_bucket = 'bvfco11'
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    now = time.localtime()

    # Get current month to not include those calls in the final CSV report
    current_month_numerical = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(0)).month

    # Get previous month for naming the final CSV report
    previous_month = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).strftime('%B')
    previous_month_numerical = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).month

    # Get current year
    current_year = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).year

    # Prefixes for keys in the S3 bucket. This will ignore any date specific file names. Add all keys to a list for
    # deletion after the program has executed
    station_standby_prefix = 'Station_Standby_Record'
    member_list_prefix = 'Member_List'
    ambulance_report_prefix = 'Ambulance_Response_Report'
    engine_report_prefix = 'Engine_Response_Report'
    chief_report_prefix = 'Chief_Response_Report'
    summary_report_prefix = 'Scheduled_Time_Report'
    finished_report_prefix = 'finished'

    # Key instantiation for file retrieval from S3 bucket
    station_standby_key = ''
    member_list_key = ''
    ambulance_report_key = ''
    engine_report_key = ''
    chief_report_key = ''
    summary_report_key = ''
    finished_key = ''
    list_of_keys = []

    number_ambulance_calls_month = 0
    number_ambulance_calls_year = 0
    number_engine_calls_month = 0
    number_engine_calls_year = 0
    number_medic_calls_month = 0
    number_medic_calls_year = 0

    station_stats_list = []

    # Iterate through all objects in the S3 bucket, and set the appropriate key names to the variables
    # instantiated above
    for obj in s3.list_objects(Bucket=s3_bucket)['Contents']:
        key = obj['Key']
        if key.startswith(station_standby_prefix):
            station_standby_key = key
            list_of_keys.append(station_standby_key)
        elif key.startswith(member_list_prefix):
            member_list_key = key
            list_of_keys.append(member_list_key)
        elif key.startswith(ambulance_report_prefix):
            ambulance_report_key = key
            list_of_keys.append(ambulance_report_key)
        elif key.startswith(engine_report_prefix):
            engine_report_key = key
            list_of_keys.append(engine_report_key)
        elif key.startswith(chief_report_prefix):
            chief_report_key = key
            list_of_keys.append(chief_report_key)
        elif key.startswith(summary_report_prefix):
            summary_report_key = key
            list_of_keys.append(summary_report_key)
        elif key.startswith(finished_report_prefix):
            finished_key = key
            list_of_keys.append(finished_key)

    # Get station standby csv from S3 bucket
    station_standby_obj = s3.get_object(Bucket=s3_bucket, Key=station_standby_key)
    station_standby_report = pd.read_csv(station_standby_obj['Body'], header=1)

    # Get member list csv from S3 bucket
    member_list_obj = s3.get_object(Bucket=s3_bucket, Key=member_list_key)
    member_list = pd.read_csv(member_list_obj['Body'], header=2)

    # Get ambulance response report csv from S3 bucket
    ambulance_report_obj = s3.get_object(Bucket=s3_bucket, Key=ambulance_report_key)
    ambulance_response_report = pd.read_csv(ambulance_report_obj['Body'], header=1)

    # Get engine response report csv from S3 bucket
    engine_report_obj = s3.get_object(Bucket=s3_bucket, Key=engine_report_key)
    engine_response_report = pd.read_csv(engine_report_obj['Body'], header=1)

    # Get chief response report csv from S3 bucket
    chief_report_obj = s3.get_object(Bucket=s3_bucket, Key=chief_report_key)
    chief_response_report = pd.read_csv(chief_report_obj['Body'], header=1)

    # Get summary report csv from S3 bucket
    summary_report_obj = s3.get_object(Bucket=s3_bucket, Key=summary_report_key)
    summary_report = pd.read_csv(summary_report_obj['Body'], header=1)

    # Initialize the dictionaries to map members to their appropriate statistics
    member_hours_dict_for_month = {}
    member_incentive_due_dict_for_month = {}
    member_ambulance_calls_dict_for_month = {}
    member_engine_calls_dict_for_month = {}
    member_chief_calls_dict_for_month = {}

    member_hours = {}
    member_did_complete_shifts = {}

    member_hours_dict_for_year = {}
    member_incentive_due_dict_for_year = {}
    member_ambulance_calls_dict_for_year = {}
    member_engine_calls_dict_for_year = {}
    member_chief_calls_dict_for_year = {}

    # Iterate through member list and initialize all members with 0 stats
    for index, row in member_list.iterrows():
        if row.loc['Member'] not in member_hours_dict_for_month:
            member_hours_dict_for_month[row.loc['Member']] = 0
        if row.loc['Member'] not in member_incentive_due_dict_for_month:
            member_incentive_due_dict_for_month[row.loc['Member']] = 0
        if row.loc['Member'] not in member_ambulance_calls_dict_for_month:
            member_ambulance_calls_dict_for_month[row.loc['Member']] = 0
        if row.loc['Member'] not in member_engine_calls_dict_for_month:
            member_engine_calls_dict_for_month[row.loc['Member']] = 0
        if row.loc['Member'] not in member_chief_calls_dict_for_month:
            member_chief_calls_dict_for_month[row.loc['Member']] = 0
        if row.loc['Member'] not in member_hours:
            member_hours[row.loc['Member']] = 0
        if row.loc['Member'] not in member_did_complete_shifts:
            member_did_complete_shifts[row.loc['Member']] = False

        if row.loc['Member'] not in member_hours_dict_for_year:
            member_hours_dict_for_year[row.loc['Member']] = 0
        if row.loc['Member'] not in member_incentive_due_dict_for_year:
            member_incentive_due_dict_for_year[row.loc['Member']] = 0
        if row.loc['Member'] not in member_ambulance_calls_dict_for_year:
            member_ambulance_calls_dict_for_year[row.loc['Member']] = 0
        if row.loc['Member'] not in member_engine_calls_dict_for_year:
            member_engine_calls_dict_for_year[row.loc['Member']] = 0
        if row.loc['Member'] not in member_chief_calls_dict_for_year:
            member_chief_calls_dict_for_year[row.loc['Member']] = 0

    # Iterate through station standby report and calculate total hours volunteered by each member
    for index2, row in station_standby_report.iterrows():
        date_in = row.loc['Date In']
        month_of_standby = int(date_in.split('-')[1])

        if month_of_standby == previous_month_numerical:
            member_hours_dict_for_month[row.loc['Submitted By']] += row.loc['Total Number of Hours']
            member_hours_dict_for_year[row.loc['Submitted By']] += row.loc['Total Number of Hours']
        else:
            if month_of_standby != current_month_numerical:
                member_hours_dict_for_year[row.loc['Submitted By']] += row.loc['Total Number of Hours']

    # Iterate through ambulance response report and calculate total ambulance incentive and
    # ambulance calls taken by each member
    for index3, row in ambulance_response_report.iterrows():
        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call != current_month_numerical:
            number_ambulance_calls_year += 1

        if month_of_call == previous_month_numerical:
            number_ambulance_calls_month += 1
            if row.loc['Transport'] == 'No':
                member_incentive_due_dict_for_month[row.loc['Driver']] += 5
                member_incentive_due_dict_for_month[row.loc['Aide/OIC']] += 5
                member_incentive_due_dict_for_year[row.loc['Driver']] += 5
                member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 5
            elif row.loc['Transport'] == 'Yes':
                member_incentive_due_dict_for_month[row.loc['Driver']] += 10
                member_incentive_due_dict_for_month[row.loc['Aide/OIC']] += 10
                member_incentive_due_dict_for_year[row.loc['Driver']] += 10
                member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 10
            else:
                member_incentive_due_dict_for_month[row.loc['Driver']] += 5
                member_incentive_due_dict_for_month[row.loc['Aide/OIC']] += 5
                member_incentive_due_dict_for_year[row.loc['Driver']] += 5
                member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 5

            member_ambulance_calls_dict_for_month[row.loc['Driver']] += 1
            member_ambulance_calls_dict_for_month[row.loc['Aide/OIC']] += 1
            member_ambulance_calls_dict_for_year[row.loc['Driver']] += 1
            member_ambulance_calls_dict_for_year[row.loc['Aide/OIC']] += 1
            if not (pd.isnull(row.loc['3rd'])):
                member_ambulance_calls_dict_for_month[row.loc['3rd']] += 1
                member_ambulance_calls_dict_for_year[row.loc['3rd']] += 1
            if not (pd.isnull(row.loc['Additional Crew'])):
                member_ambulance_calls_dict_for_month[row.loc['Additional Crew']] += 1
                member_ambulance_calls_dict_for_year[row.loc['Additional Crew']] += 1
        else:
            if month_of_call != current_month_numerical:
                if row.loc['Transport'] == 'No':
                    member_incentive_due_dict_for_year[row.loc['Driver']] += 5
                    member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 5
                elif row.loc['Transport'] == 'Yes':
                    member_incentive_due_dict_for_year[row.loc['Driver']] += 10
                    member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 10
                else:
                    member_incentive_due_dict_for_year[row.loc['Driver']] += 5
                    member_incentive_due_dict_for_year[row.loc['Aide/OIC']] += 5

                member_ambulance_calls_dict_for_year[row.loc['Driver']] += 1
                member_ambulance_calls_dict_for_year[row.loc['Aide/OIC']] += 1
                if not (pd.isnull(row.loc['3rd'])):
                    member_ambulance_calls_dict_for_year[row.loc['3rd']] += 1
                if not (pd.isnull(row.loc['Additional Crew'])):
                    member_ambulance_calls_dict_for_year[row.loc['Additional Crew']] += 1

    # Iterate through engine response report and calculate total engine incentive and engine calls taken by each member
    for index4, row in engine_response_report.iterrows():
        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call != current_month_numerical:
            number_engine_calls_year += 1
        if month_of_call == previous_month_numerical:
            number_engine_calls_month += 1
            member_incentive_due_dict_for_month[row.loc['Driver']] += 5
            member_incentive_due_dict_for_month[row.loc['Officer-In-Charge']] += 5
            member_incentive_due_dict_for_year[row.loc['Driver']] += 5
            member_incentive_due_dict_for_year[row.loc['Officer-In-Charge']] += 5

            member_engine_calls_dict_for_month[row.loc['Driver']] += 1
            member_engine_calls_dict_for_month[row.loc['Officer-In-Charge']] += 1
            member_engine_calls_dict_for_year[row.loc['Driver']] += 1
            member_engine_calls_dict_for_year[row.loc['Officer-In-Charge']] += 1
            if not (pd.isnull(row.loc['Line'])):
                member_engine_calls_dict_for_month[row.loc['Line']] += 1
                member_engine_calls_dict_for_year[row.loc['Line']] += 1
            if not (pd.isnull(row.loc['Backup'])):
                member_engine_calls_dict_for_month[row.loc['Backup']] += 1
                member_engine_calls_dict_for_year[row.loc['Backup']] += 1
            if not (pd.isnull(row.loc['Bars'])):
                member_engine_calls_dict_for_month[row.loc['Bars']] += 1
                member_engine_calls_dict_for_year[row.loc['Bars']] += 1
            if not (pd.isnull(row.loc['Layout'])):
                member_engine_calls_dict_for_month[row.loc['Layout']] += 1
                member_engine_calls_dict_for_year[row.loc['Layout']] += 1
            if not (pd.isnull(row.loc['Observer'])):
                member_engine_calls_dict_for_month[row.loc['Observer']] += 1
                member_engine_calls_dict_for_year[row.loc['Observer']] += 1
        else:
            if month_of_call != current_month_numerical:
                member_incentive_due_dict_for_year[row.loc['Driver']] += 5
                member_incentive_due_dict_for_year[row.loc['Officer-In-Charge']] += 5

                member_engine_calls_dict_for_year[row.loc['Driver']] += 1
                member_engine_calls_dict_for_year[row.loc['Officer-In-Charge']] += 1
                if not (pd.isnull(row.loc['Line'])):
                    member_engine_calls_dict_for_year[row.loc['Line']] += 1
                if not (pd.isnull(row.loc['Backup'])):
                    member_engine_calls_dict_for_year[row.loc['Backup']] += 1
                if not (pd.isnull(row.loc['Bars'])):
                    member_engine_calls_dict_for_year[row.loc['Bars']] += 1
                if not (pd.isnull(row.loc['Layout'])):
                    member_engine_calls_dict_for_year[row.loc['Layout']] += 1
                if not (pd.isnull(row.loc['Observer'])):
                    member_engine_calls_dict_for_year[row.loc['Observer']] += 1

    # Iterate through chief response report and calculate total chief calls taken by each member
    for index5, row in chief_response_report.iterrows():
        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call == previous_month_numerical:
            if not (pd.isnull(row.loc['Chief'])):
                member_chief_calls_dict_for_month[row.loc['Chief']] += 1
                member_chief_calls_dict_for_year[row.loc['Chief']] += 1
            if not (pd.isnull(row.loc['Aide'])):
                member_chief_calls_dict_for_month[row.loc['Aide']] += 1
                member_chief_calls_dict_for_year[row.loc['Aide']] += 1
        else:
            if month_of_call != current_month_numerical:
                if not (pd.isnull(row.loc['Chief'])):
                    member_chief_calls_dict_for_year[row.loc['Chief']] += 1
                if not (pd.isnull(row.loc['Aide'])):
                    member_chief_calls_dict_for_year[row.loc['Aide']] += 1

    # Iterate through station scheduled hours report and check if member completed the three required duty shifts for
    # the month
    for index6, row in summary_report.iterrows():
        acceptable_schedules = ["1st Out Ambo", "2nd Out Ambo", "Wagon", "Non-Operational Observers"]

        if row.loc['Schedule'] in acceptable_schedules:
            if row.loc['Member'] in member_hours:
                member_hours[row.loc['Member']] += row.loc['Total Hours']
            else:
                member_hours[row.loc['Member']] = row.loc['Total Hours']

            if member_hours[row.loc['Member']] >= 36:
                member_did_complete_shifts[row.loc["Member"]] = True
            else:
                member_did_complete_shifts[row.loc["Member"]] = False

    # Create text file with station stats for month and year
    station_stats_list.append("Ambulance calls for " + str(previous_month) + ": " + str(number_ambulance_calls_month) +
                              "\n")
    station_stats_list.append("Engine calls for " + str(previous_month) + ": " + str(number_engine_calls_month) + "\n")
    station_stats_list.append("Medic calls for " + str(previous_month) + ": " + str(number_medic_calls_month) + "\n\n")
    station_stats_list.append("Ambulance calls for " + str(current_year) + ": " + str(number_ambulance_calls_year) +
                              "\n")
    station_stats_list.append("Engine calls for " + str(current_year) + ": " + str(number_engine_calls_year) + "\n")
    station_stats_list.append("Medic calls for " + str(current_year) + ": " + str(number_medic_calls_year) + "\n\n")

    # Create string of station statistics to convert into text file
    station_stats_file = ''
    for line in station_stats_list:
        station_stats_file += line

    # Initialize member lists and stats for conversion into Python DataFrame
    member_names = []
    member_completed_shifts_for_month = []
    member_hours_for_month = []
    member_incentive_for_month = []
    member_ambulance_calls_for_month = []
    member_engine_calls_for_month = []
    member_chief_calls_for_month = []
    empty_list = []
    member_hours_for_year = []
    member_incentive_for_year = []
    member_ambulance_calls_for_year = []
    member_engine_calls_for_year = []
    member_chief_calls_for_year = []

    # Add all members and their associated stats into the appropriate list
    for member in member_hours_dict_for_month:
        member_names.append(member)
        member_completed_shifts_for_month.append(member_did_complete_shifts[member])
        member_hours_for_month.append(member_hours_dict_for_month[member])
        member_incentive_for_month.append(member_incentive_due_dict_for_month[member])
        member_ambulance_calls_for_month.append(member_ambulance_calls_dict_for_month[member])
        member_engine_calls_for_month.append(member_engine_calls_dict_for_month[member])
        member_chief_calls_for_month.append(member_chief_calls_dict_for_month[member])
        empty_list.append(None)
        member_hours_for_year.append(member_hours_dict_for_year[member])
        member_incentive_for_year.append(member_incentive_due_dict_for_year[member])
        member_ambulance_calls_for_year.append(member_ambulance_calls_dict_for_year[member])
        member_engine_calls_for_year.append(member_engine_calls_dict_for_year[member])
        member_chief_calls_for_year.append(member_chief_calls_dict_for_year[member])

    # Set dictionary for final CSV report labels
    member_hour_final_dict = {
        'Member': member_names,
        'Completed 3 shifts in ' + str(previous_month): member_completed_shifts_for_month,
        'Station Standby Hours Reported in ' + str(previous_month): member_hours_for_month,
        'Incentive Due in ' + str(previous_month): member_incentive_for_month,
        'Ambulance Calls Taken in ' + str(previous_month): member_ambulance_calls_for_month,
        'Engine Calls Taken in ' + str(previous_month): member_engine_calls_for_month,
        'Chief Calls Taken in ' + str(previous_month): member_chief_calls_for_month,
        '': empty_list,
        'Station Standby Hours Reported in ' + str(current_year): member_hours_for_year,
        'Incentive Due in ' + str(current_year): member_incentive_for_year,
        'Ambulance Calls Taken in ' + str(current_year): member_ambulance_calls_for_year,
        'Engine Calls Taken in ' + str(current_year): member_engine_calls_for_year,
        'Chief Calls Taken in ' + str(current_year): member_chief_calls_for_year
    }

    # Set the report title and initialize a buffer for conversion into a CSV
    report_upload = str(previous_month) + '_Station_11_Report.csv'
    csv_buffer = StringIO()

    # Convert the data from lists into a Python DataFrame
    member_hours_dataframe = pd.DataFrame(member_hour_final_dict)

    # Convert the Python DataFrame into a CSV using Pandas
    member_hours_dataframe.to_csv(csv_buffer, index=False)

    # Create S3-approved text file format for upload
    station_standby_file_upload = StringIO(station_stats_file)

    # Put request for final CSV report into S3 bucket and the station statistics text file
    s3_resource.Object(s3_bucket, report_upload).put(Body=csv_buffer.getvalue())
    s3_resource.Object(s3_bucket, 'station_stats.txt').put(Body=station_standby_file_upload.read())

    # Delete all files that are not needed anymore
    for key in list_of_keys:
        s3_resource.Object(s3_bucket, key).delete()

    # Return success
    return 0

