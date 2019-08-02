class Standby:
    """ Standby Class to consolidate member with all standby data for month """

    # Member attributes: {'Member Name': ['Date In': 'Date', 'Time In': 'Time In', 'Time Out': 'Time Out',
    # 'Total Number of Hours', 0] }

    def __init__(self):
        self.member_name = ''
        self.date_in = ''
        self.time_in = ''
        self.time_out = ''
        self.total_hours = 0

    def set_attributes(self, member_name, date_in, time_in, time_out, total_hours):
        self.member_name = member_name
        self.date_in = date_in
        self.time_in = time_in
        self.time_out = time_out
        self.total_hours = total_hours

