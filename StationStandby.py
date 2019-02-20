import pandas as pd

file_save_location_path = '/Users/RaviKothari/Documents/Ravi/Important/BVFCO 11/'

station_standby_report = pd.read_csv('Station_Standby_Jan.csv', header=1)
member_list = pd.read_csv('Member_List.csv', header=2)

member_hours_dict = {}

for index, row in station_standby_report.iterrows():
    if row['Submitted By'] not in member_hours_dict:
        member_hours_dict[row['Submitted By']] = row['Total Number of Hours']
    else:
        member_hours_dict[row['Submitted By']] += row['Total Number of Hours']

for index, row in member_list.iterrows():
    if row['Member'] not in member_hours_dict:
        member_hours_dict[row['Member']] = 0

member_names = []
member_hours = []

for member in member_hours_dict:
    member_names.append(member)
    member_hours.append(member_hours_dict[member])

member_hour_final_dict = {
    'Member': member_names,
    'Station Standby Hours Reported': member_hours
}

member_hours_dataframe = pd.DataFrame(member_hour_final_dict)
member_hours_dataframe.to_csv(file_save_location_path+'January_Station_Standby_Hours.csv')
