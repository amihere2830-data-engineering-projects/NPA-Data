from datetime import datetime
import re
import pandas as pd


def found_month(date):
    """
    Search for single month not month range
    """
    if '-' not in date:
        month_split =  date.split(' ')
        if len(month_split[0]) > 2:
            return date
    else:
        month_split =  date.split('-')
        if len(month_split[0]) > 2 and ' ' not in month_split[-1] :
            return date
    return False

def monthyr_to_yrmonth(dt):
    """
    Convert date format Jan, 2002 to 2002-01
    """
    return datetime.strptime(dt, '%b, %Y').strftime('%Y-%m')

def create_date(df, sName, year):
    """
    Create date column for the data frame
    """

    if str(sName) == str(year):
            df['DATE'] = year
    else:
        date  = re.findall(r'^\w*',sName)[0][0:3] + ", " + year
        df['DATE'] = monthyr_to_yrmonth(date)
    # convert date to date object
    df['DATE'] = pd.to_datetime(df['DATE'])
    return df