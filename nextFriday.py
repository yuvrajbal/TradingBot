from datetime import datetime, timedelta
import pytz

def get_next_friday():
    """Returns the next Friday's date in YYYYMMDD format."""
    today = datetime.now(pytz.timezone("America/New_York"))
    days_until_friday = (4 - today.weekday()) % 7  # Friday is weekday 4
    next_friday = today + timedelta(days=days_until_friday)
    return next_friday.strftime('%Y%m%d')

print(get_next_friday())