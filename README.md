This project was created to analyze Branchville Volunteer Fire Company's member call records. The backend of the project was written in Python, and the Excel spreadsheet was stored in Amazon S3. The Excel spreadsheet was pulled from the Branchville call report, and contains information including the call's incident number, the station run number, the location of the call, the time the unit was dispatched, the time the unit came back to the station, the type of the incident, the first due company of where the call was, the unit used, the disposition, whether or not a transport was completed, the eMeds report number, the driver, the aide, the 3rd (if applicable), any observers (if applicable), any notes written.
In order to automate the processing of the Excel spreadsheet, AWS Lambda will be used. The lambda function will be triggered every 30 days.

Next steps for this project include:
*Fixing minor bugs involving station stats text file generation
*Introducing a database component to prevent the same calculations from re-occuring every month
*Developing a front-end component to allow users to upload and download files without having access to AWS technology
