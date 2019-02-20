import pandas as pd

file_save_location_path = "/Users/RaviKothari/Documents/Ravi/Important/BVFCO 11/"

ambulance_response_report = pd.read_csv("Ambulance_Response_Report_Jan.csv", header=1)
engine_response_report = pd.read_csv("Engine_Response_Report_Jan.csv", header=1)

member_incentive_due_dict = {}

for index, row in ambulance_response_report.iterrows():

    if row['Driver'] not in member_incentive_due_dict:
        member_incentive_due_dict[row['Driver']] = 0
    if row['Aide/OIC'] not in member_incentive_due_dict:
        member_incentive_due_dict[row['Aide/OIC']] = 0

for index, row in engine_response_report.iterrows():

    if row['Driver'] not in member_incentive_due_dict:
        member_incentive_due_dict[row['Driver']] = 0
    if row['Officer-In-Charge'] not in member_incentive_due_dict:
        member_incentive_due_dict[row['Officer-In-Charge']] = 0

for index, row in ambulance_response_report.iterrows():

    if row['Transport'] == 'No':
        member_incentive_due_dict[row['Driver']] += 5
        member_incentive_due_dict[row['Aide/OIC']] += 5
    elif row['Transport'] == 'Yes':
        member_incentive_due_dict[row['Driver']] += 10
        member_incentive_due_dict[row['Aide/OIC']] += 10
    else:
        member_incentive_due_dict[row['Driver']] += 5
        member_incentive_due_dict[row['Aide/OIC']] += 5

for index, row in engine_response_report.iterrows():

    member_incentive_due_dict[row['Driver']] += 5
    member_incentive_due_dict[row['Officer-In-Charge']] += 5

member_names = []
member_incentive = []

for member in member_incentive_due_dict:
    member_names.append(member)
    member_incentive.append(member_incentive_due_dict[member])

final_incentive_stats = {
    'Member': member_names,
    'Incentive Due': member_incentive
}

member_incentive_dataframe = pd.DataFrame(final_incentive_stats)
member_incentive_dataframe.to_csv(file_save_location_path+"January_Incentive_Report.csv")