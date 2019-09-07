import datetime
import time
import boto3
import pandas as pd
from io import StringIO
from Standby import Standby
from boto3.dynamodb.conditions import Key

# S3 credentials

s3_bucket = 'bvfco11'
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# DynamoDB credentials

dynamodb_resource = boto3.resource('dynamodb')
dynamodb_table = dynamodb_resource.Table('TestMemberStatistics')


def get_date_information():
    now = time.localtime()

    # Get current month to not include those calls in the final CSV report
    current_month_numerical = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(0)).month

    # Get previous month for naming the final CSV report
    previous_month = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).strftime('%B')
    previous_month_numerical = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).month

    # Get current year
    current_year = (datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)).year

    date_information = {
        'CURRENT_TIME': now,
        'CURRENT_MONTH_NUMERICAL': current_month_numerical,
        'PREVIOUS_MONTH': previous_month,
        'PREVIOUS_MONTH_NUMERICAL': previous_month_numerical,
        'CURRENT_YEAR': current_year
    }

    return date_information


def instantiate_keys():
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
    for obj in s3_client.list_objects(Bucket=s3_bucket)['Contents']:
        key = obj['Key']
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
        elif key.startswith(finished_report_prefix):
            finished_key = key

    keys = {
        "Station_Standby": station_standby_key,
        "Member_List": member_list_key,
        "Ambulance_Report": ambulance_report_key,
        "Engine_Report": engine_report_key,
        "Chief_Report": chief_report_key,
        "Scheduled_Report": scheduled_report_key,
        "Finished": finished_key
    }

    return keys


def get_report_csv(key, header_number):
    obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
    return pd.read_csv(obj['Body'], header=header_number)


def initialize_member_obj(member_list, member_hours_dict_for_month, member_incentive_due_dict_for_month,
                          member_ambulance_calls_dict_for_month, member_engine_calls_dict_for_month,
                          member_chief_calls_dict_for_month, member_hours, member_did_complete_shifts,
                          member_hours_dict_for_year, member_incentive_due_dict_for_year,
                          member_ambulance_calls_dict_for_year, member_engine_calls_dict_for_year,
                          member_chief_calls_dict_for_year):

    member_member_id_relationship = {}

    # Iterate through member list and initialize all members with 0 stats
    for index, row in member_list.iterrows():
        member_id = str(row.loc['Dept ID'])
        member_name = str(row.loc['Member'])

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

        # Create empty entry in table for all users if it does not exist

        count = dynamodb_table.query(
            Select='COUNT',
            KeyConditionExpression=Key('ID_Number').eq(member_id)

        )

        if count['Count'] == 0:
            dynamodb_table.put_item(
                Item={
                    'ID_Number': member_id,
                    'Member Name': member_name,
                    'Number Missed Duty Shift Months': '0',
                    'Warning Level': 'green'
                }
            )

        member_member_id_relationship[member_name] = member_id

    return member_member_id_relationship


def calculate_standby_hours(station_standby_report, member_hours_dict_for_month, member_hours_dict_for_year, standby,
                            all_date_information):
    # Iterate through station standby report and calculate total hours volunteered by each member
    for index, row in station_standby_report.iterrows():
        date_in = row.loc['Date In']
        month_of_standby = int(date_in.split('-')[1])

        if month_of_standby == all_date_information["PREVIOUS_MONTH_NUMERICAL"]:
            member_hours_dict_for_month[row.loc['Submitted By']] += row.loc['Total Number of Hours']
            member_hours_dict_for_year[row.loc['Submitted By']] += row.loc['Total Number of Hours']
        else:
            if month_of_standby != all_date_information["CURRENT_MONTH_NUMERICAL"]:
                member_hours_dict_for_year[row.loc['Submitted By']] += row.loc['Total Number of Hours']

        current_member_standby_obj = Standby()
        current_member_standby_obj.set_attributes(row.loc['Submitted By'], str(row.loc['Date In']),
                                                  str(row.loc['Time In']), str(row.loc['Time Out']),
                                                  row.loc['Total Number of Hours'])

        if row.loc['Submitted By'] not in standby:
            standby[row.loc['Submitted By']] = []

        standby[row.loc['Submitted By']].append(current_member_standby_obj)

    return standby


def calculate_ambulance_stats(ambulance_response_report,
                              member_incentive_due_dict_for_month, member_incentive_due_dict_for_year,
                              member_ambulance_calls_dict_for_month, member_ambulance_calls_dict_for_year,
                              all_date_information):
    number_ambulance_calls_month = 0
    number_ambulance_calls_year = 0

    # Iterate through ambulance response report and calculate total ambulance incentive and ambulance calls taken by
    # each member
    for index, row in ambulance_response_report.iterrows():
        if row.loc['Station Run Number/R#'] == 0:
            continue

        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call != all_date_information["CURRENT_MONTH_NUMERICAL"]:
            number_ambulance_calls_year += 1

        if month_of_call == all_date_information["PREVIOUS_MONTH_NUMERICAL"]:
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
            if month_of_call != all_date_information["CURRENT_MONTH_NUMERICAL"]:
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

    ambulance_station_stats = {
        'Ambulance_Calls_Month': number_ambulance_calls_month,
        'Ambulance_Calls_Year': number_ambulance_calls_year
    }

    return ambulance_station_stats


def calculate_engine_stats(engine_response_report,
                           member_incentive_due_dict_for_month, member_incentive_due_dict_for_year,
                           member_engine_calls_dict_for_month, member_engine_calls_dict_for_year, all_date_information):
    number_engine_calls_month = 0
    number_engine_calls_year = 0

    # Iterate through engine response report and calculate total engine incentive and engine calls taken by each member
    for index, row in engine_response_report.iterrows():
        if row.loc['Station Run Number/F#'] == 0:
            continue

        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call != all_date_information["CURRENT_MONTH_NUMERICAL"]:
            number_engine_calls_year += 1
        if month_of_call == all_date_information["PREVIOUS_MONTH_NUMERICAL"]:
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
            if month_of_call != all_date_information["CURRENT_MONTH_NUMERICAL"]:
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

    engine_station_stats = {
        'Engine_Calls_Month': number_engine_calls_month,
        'Engine_Calls_Year': number_engine_calls_year
    }

    return engine_station_stats


def calculate_chief_stats(chief_response_report, member_chief_calls_dict_for_month, member_chief_calls_dict_for_year,
                          all_date_information):
    # Iterate through chief response report and calculate total chief calls taken by each member
    for index, row in chief_response_report.iterrows():
        date_dispatched = row.loc['Date Dispatched']
        month_of_call = int(date_dispatched.split('-')[1])

        if month_of_call == all_date_information["PREVIOUS_MONTH_NUMERICAL"]:
            if not (pd.isnull(row.loc['Chief'])):
                member_chief_calls_dict_for_month[row.loc['Chief']] += 1
                member_chief_calls_dict_for_year[row.loc['Chief']] += 1
            if not (pd.isnull(row.loc['Aide'])):
                member_chief_calls_dict_for_month[row.loc['Aide']] += 1
                member_chief_calls_dict_for_year[row.loc['Aide']] += 1
        else:
            if month_of_call != all_date_information["CURRENT_MONTH_NUMERICAL"]:
                if not (pd.isnull(row.loc['Chief'])):
                    member_chief_calls_dict_for_year[row.loc['Chief']] += 1
                if not (pd.isnull(row.loc['Aide'])):
                    member_chief_calls_dict_for_year[row.loc['Aide']] += 1


def verify_duty_shift_completion(summary_report, member_hours, member_did_complete_shifts, member_member_id,
                                 warning_level):
    # Iterate through station scheduled hours report and check if member completed the three required duty shifts for
    # the month
    for index, row in summary_report.iterrows():
        acceptable_schedules = ['1st Out Ambo', '2nd Out Ambo', 'Wagon', 'Non-Operational Observers']

        if row.loc['Schedule'] in acceptable_schedules:
            if row.loc['Member'] in member_hours:
                member_hours[row.loc['Member']] += row.loc['Total Hours']
            else:
                member_hours[row.loc['Member']] = row.loc['Total Hours']

            if member_hours[row.loc['Member']] >= 36:
                member_did_complete_shifts[row.loc['Member']] = True
            else:
                member_did_complete_shifts[row.loc['Member']] = False

    update_database_for_duty_shift_completion(member_did_complete_shifts, member_member_id, warning_level)


def update_database_for_duty_shift_completion(member_did_complete_shifts, member_member_id, warning_level):
    for member in member_did_complete_shifts:
        if not member_did_complete_shifts[member]:
            member_query = dynamodb_table.query(
                KeyConditionExpression=Key('ID_Number').eq(member_member_id[member])
            )

            num_missed_duty_shifts = int(member_query['Items'][0]['Number Missed Duty Shift Months'])

            if num_missed_duty_shifts < 3:
                num_missed_duty_shifts += 1

            if member_query is not None:
                dynamodb_table.put_item(
                    Item={
                        'ID_Number': member_query['Items'][0]['ID_Number'],
                        'Member Name': member_query['Items'][0]['Member Name'],
                        'Number Missed Duty Shift Months': str(num_missed_duty_shifts),
                        'Warning Level': warning_level[num_missed_duty_shifts]
                    }
                )


def create_station_stats_text_file(ambulance_stats, engine_stats, medic_stats, all_date_information):
    station_stats_file = ''

    station_stats_list = ["Ambulance calls for " + str(all_date_information['PREVIOUS_MONTH']) + ": " +
                          str(ambulance_stats['Ambulance_Calls_Month']) + "\n",
                          "Engine calls for " + str(all_date_information['PREVIOUS_MONTH']) + ": " +
                          str(engine_stats['Engine_Calls_Month']) + "\n",
                          "Medic calls for " + str(all_date_information['PREVIOUS_MONTH']) + ": " +
                          str(medic_stats['Medic_Calls_Month']) + "\n\n",
                          "Ambulance calls for " + str(all_date_information['CURRENT_YEAR']) + ": " +
                          str(ambulance_stats['Ambulance_Calls_Year']) + "\n",
                          "Engine calls for " + str(all_date_information['PREVIOUS_MONTH']) + ": " +
                          str(engine_stats['Engine_Calls_Year']) + "\n",
                          "Medic calls for " + str(all_date_information['PREVIOUS_MONTH']) + ": " +
                          str(medic_stats['Medic_Calls_Year']) + "\n\n"]
    # Create text file with station stats for month and year

    for line in station_stats_list:
        station_stats_file += line

    return StringIO(station_stats_file)


def create_final_analytics_report(member_hours_dict_for_month, member_did_complete_shifts,
                                  member_incentive_due_dict_for_month, member_ambulance_calls_dict_for_month,
                                  member_engine_calls_dict_for_month, member_chief_calls_dict_for_month,
                                  member_hours_dict_for_year, member_incentive_due_dict_for_year,
                                  member_ambulance_calls_dict_for_year, member_engine_calls_dict_for_year,
                                  member_chief_calls_dict_for_year, csv_buffer, member_member_id, all_date_information):
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
    number_duty_shift_months_missed = []
    warning_level = []

    # Add all members and their associated stats into the appropriate list
    for member in member_hours_dict_for_month:

        member_dynamo_query = dynamodb_table.query(
                KeyConditionExpression=Key('ID_Number').eq(member_member_id[member]))
        number_duty_shift_months_missed_for_member = int(member_dynamo_query['Items'][0]['Number Missed Duty Shift '
                                                                                         'Months'])
        warning_level_for_member = str(member_dynamo_query['Items'][0]['Warning Level'])

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
        number_duty_shift_months_missed.append(number_duty_shift_months_missed_for_member)
        warning_level.append(warning_level_for_member)

    # Set dictionary for final CSV report labels
    member_hour_final_dict = {
        'Member': member_names,
        'Completed 3 shifts in ' + str(all_date_information['PREVIOUS_MONTH']) + '?': member_completed_shifts_for_month,
        'Total Number of Missed Duty Shift Months': number_duty_shift_months_missed,
        'Warning Level': warning_level,
        'Station Standby Hours Reported in ' + str(all_date_information['PREVIOUS_MONTH']): member_hours_for_month,
        'Incentive Due in ' + str(all_date_information['PREVIOUS_MONTH']): member_incentive_for_month,
        'Ambulance Calls Taken in ' + str(all_date_information['PREVIOUS_MONTH']): member_ambulance_calls_for_month,
        'Engine Calls Taken in ' + str(all_date_information['PREVIOUS_MONTH']): member_engine_calls_for_month,
        'Chief Calls Taken in ' + str(all_date_information['PREVIOUS_MONTH']): member_chief_calls_for_month,
        '': empty_list,
        'Station Standby Hours Reported in ' + str(all_date_information['CURRENT_YEAR']): member_hours_for_year,
        'Incentive Due in ' + str(all_date_information['CURRENT_YEAR']): member_incentive_for_year,
        'Ambulance Calls Taken in ' + str(all_date_information['CURRENT_YEAR']): member_ambulance_calls_for_year,
        'Engine Calls Taken in ' + str(all_date_information['CURRENT_YEAR']): member_engine_calls_for_year,
        'Chief Calls Taken in ' + str(all_date_information['CURRENT_YEAR']): member_chief_calls_for_year
    }

    # Convert the data from lists into a Python DataFrame
    member_hours_dataframe = pd.DataFrame(member_hour_final_dict)

    # Convert the Python DataFrame into a CSV using Pandas
    member_hours_dataframe.to_csv(csv_buffer, index=False)

    # Returns True if no error arises in CSV report building
    return True


def put_objects_in_s3(report_upload_name, csv_buffer):
    s3_resource.Object(s3_bucket, report_upload_name).put(Body=csv_buffer.getvalue())


def delete_redundant_keys(keys):
    # Delete all files that are not needed anymore
    for key in keys.values():
        s3_resource.Object(s3_bucket, key).delete()


def consolidate_probationary_member_goals(member_list, probationary_member_goals, station_standby_report,
                                          all_date_information):
    for index, row in member_list.iterrows():
        if row.loc['Membership Type'] == 'Probationary':
            probationary_member_goals[row.loc['Member']] = []

    for index, row in station_standby_report.iterrows():
        if row.loc['Submitted By'] in probationary_member_goals:
            date_in = row.loc['Date In']
            month_of_standby = int(date_in.split('-')[1])

            if month_of_standby == all_date_information["PREVIOUS_MONTH_NUMERICAL"]:
                if type(row.loc['What did you accomplish?']) is str:
                    probationary_member_goals[row.loc['Submitted By']].append('*' +
                                                                              row.loc['What did you accomplish?'])


def create_probationary_goal_report(probationary_member_goals, csv_buffer):
    member_names = []
    member_individual_goals = []

    for member in probationary_member_goals:
        if len(probationary_member_goals[member]) == 0:
            probationary_member_goals[member] = '-'
        elif len(probationary_member_goals[member]) == 1:
            probationary_member_goals[member] = str(probationary_member_goals[member][0])
        else:
            all_goals = ''
            for goal in probationary_member_goals[member]:
                all_goals += goal + '\n'
            all_goals = all_goals[:-2]
            probationary_member_goals[member] = all_goals

    for member in probationary_member_goals:
        member_names.append(member)
        member_individual_goals.append(probationary_member_goals[member])

    member_goals_final_dict = {
        'Member': member_names,
        'Goals': member_individual_goals
    }

    member_goals_dataframe = pd.DataFrame(member_goals_final_dict)

    member_goals_dataframe.to_csv(csv_buffer, index=False)

    # Returns True if no error arises in CSV report building
    return True


# Lambda function that will run each time each time a PUT request is done in the 'bvfco11' S3 bucket. This function
# will calculate member statistics, including number of ambulance runs, number of engine runs, hours volunteered, etc.
def my_lambda_handler(event, context):
    all_date_information = get_date_information()

    keys = instantiate_keys()

    # Get station standby csv from S3 bucket
    station_standby_report = get_report_csv(keys["Station_Standby"], 1)

    # Get member list csv from S3 bucket
    member_list = get_report_csv(keys["Member_List"], 2)

    # Get ambulance response report csv from S3 bucket
    ambulance_response_report = get_report_csv(keys["Ambulance_Report"], 1)

    # Get engine response report csv from S3 bucket
    engine_response_report = get_report_csv(keys["Engine_Report"], 1)

    # Get chief response report csv from S3 bucket
    chief_response_report = get_report_csv(keys['Chief_Report'], 1)

    # Get scheduled report csv from S3 bucket
    scheduled_report = get_report_csv(keys["Scheduled_Report"], 1)

    # Initialize the dictionaries to map members to their appropriate statistics
    member_hours_dict_for_month = {}
    member_incentive_due_dict_for_month = {}
    member_ambulance_calls_dict_for_month = {}
    member_engine_calls_dict_for_month = {}
    member_chief_calls_dict_for_month = {}

    member_hours = {}
    member_did_complete_shifts = {}
    probationary_member_goals = {}

    member_hours_dict_for_year = {}
    member_incentive_due_dict_for_year = {}
    member_ambulance_calls_dict_for_year = {}
    member_engine_calls_dict_for_year = {}
    member_chief_calls_dict_for_year = {}

    standby = {}

    warning_level = {
        0: 'green',
        1: 'yellow',
        2: 'red',
        3: 'black'
    }

    member_member_id = initialize_member_obj(member_list, member_hours_dict_for_month,
                                             member_incentive_due_dict_for_month, member_ambulance_calls_dict_for_month,
                                             member_engine_calls_dict_for_month, member_chief_calls_dict_for_month,
                                             member_hours, member_did_complete_shifts, member_hours_dict_for_year,
                                             member_incentive_due_dict_for_year, member_ambulance_calls_dict_for_year,
                                             member_engine_calls_dict_for_year, member_chief_calls_dict_for_year)

    calculate_standby_hours(station_standby_report, member_hours_dict_for_month, member_hours_dict_for_year, standby,
                            all_date_information)

    ambulance_stats = calculate_ambulance_stats(ambulance_response_report, member_incentive_due_dict_for_month,
                                                member_incentive_due_dict_for_year,
                                                member_ambulance_calls_dict_for_month,
                                                member_ambulance_calls_dict_for_year, all_date_information)

    engine_stats = calculate_engine_stats(engine_response_report, member_incentive_due_dict_for_month,
                                          member_incentive_due_dict_for_year, member_engine_calls_dict_for_month,
                                          member_engine_calls_dict_for_year, all_date_information)

    calculate_chief_stats(chief_response_report, member_chief_calls_dict_for_month, member_chief_calls_dict_for_year,
                          all_date_information)

    verify_duty_shift_completion(scheduled_report, member_hours, member_did_complete_shifts, member_member_id, warning_level)

    consolidate_probationary_member_goals(member_list, probationary_member_goals, station_standby_report,
                                          all_date_information)

    # To be replaced upon medic reports being submitted
    medic_stats = {
        "Medic_Calls_Month": 0,
        "Medic_Calls_Year": 0
    }

    # Set the analytics report title
    analytics_report_upload = str(all_date_information['PREVIOUS_MONTH']) + '_Station_11_Report.csv'

    # Set the station stats text file
    station_stats_text_upload = str(all_date_information['PREVIOUS_MONTH']) + '_Station_Stats.txt'

    # Set the probationary goals report title
    probationary_report_upload = str(all_date_information['PREVIOUS_MONTH']) + '_Probationary_Member_Goals.csv'

    csv_buffer = StringIO()

    create_final_analytics_report(member_hours_dict_for_month, member_did_complete_shifts,
                                  member_incentive_due_dict_for_month, member_ambulance_calls_dict_for_month,
                                  member_engine_calls_dict_for_month, member_chief_calls_dict_for_month,
                                  member_hours_dict_for_year, member_incentive_due_dict_for_year,
                                  member_ambulance_calls_dict_for_year, member_engine_calls_dict_for_year,
                                  member_chief_calls_dict_for_year, csv_buffer, member_member_id, all_date_information)
    put_objects_in_s3(analytics_report_upload, csv_buffer)

    station_standby_file_upload = create_station_stats_text_file(ambulance_stats, engine_stats, medic_stats,
                                                                 all_date_information)

    # TODO
    s3_resource.Object(s3_bucket, station_stats_text_upload).put(Body=station_standby_file_upload.read())

    csv_buffer = StringIO()
    create_probationary_goal_report(probationary_member_goals, csv_buffer)
    put_objects_in_s3(probationary_report_upload, csv_buffer)

    delete_redundant_keys(keys)

    # Return success
    return 0

