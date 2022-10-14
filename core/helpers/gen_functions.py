from datetime import date, timedelta


def get_current_time():
    """
    Get current time from UTC in naive defined by server, following a 
    specific format required by DB
    """
    time_now = date.today() + timedelta(days=5)
    return time_now.strftime("%d-%m-%Y %H:%M:%S")