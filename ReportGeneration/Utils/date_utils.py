import time
import datetime as dt


def get_date_information():
    now = time.localtime()

    # Get current month to not include those calls in the final CSV report
    current_month_numerical = (dt.date(now.tm_year, now.tm_mon, 1) - dt.timedelta(0)).month

    # Get previous month for naming the final CSV report
    previous_month = (dt.date(now.tm_year, now.tm_mon, 1) - dt.timedelta(1)).strftime('%B')
    previous_month_numerical = (dt.date(now.tm_year, now.tm_mon, 1) - dt.timedelta(1)).month

    # Get current year
    current_year = (dt.date(now.tm_year, now.tm_mon, 1) - dt.timedelta(1)).year

    date_information = {
        'CURRENT_TIME': now,
        'CURRENT_MONTH_NUMERICAL': current_month_numerical,
        'PREVIOUS_MONTH': previous_month,
        'PREVIOUS_MONTH_NUMERICAL': previous_month_numerical,
        'CURRENT_YEAR': current_year
    }

    return date_information

