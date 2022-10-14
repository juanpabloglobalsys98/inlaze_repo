import pytz


def timezone_customer(datetime):
    bogota = pytz.timezone('America/Bogota')
    date_customer = datetime.astimezone(bogota)
    return date_customer