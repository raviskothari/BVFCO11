import csv
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CSVUtils:
    def __init__(self):
        pass

    def read_s3_object(self, s3_object_full, header_number):
        s3_object = s3_object_full.get()
        lines = s3_object['Body'].read().decode('utf-8').split()

        csv_reader = csv.reader(lines)
        header = list(csv_reader)[header_number]

        all_rows = []

        num_rows_to_skip = header_number

        # now iterate over those lines
        for row in csv.DictReader(lines, fieldnames=header):
            if num_rows_to_skip >= 0:
                num_rows_to_skip -= 1
                continue

            # here you get a sequence of dicts
            # do whatever you want with each line here
            all_rows.append(row)

        return all_rows
