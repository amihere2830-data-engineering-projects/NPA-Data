from datetime import datetime
import re
import pandas as pd
import calendar


def months():
    """
    Get list of months
    """
    all_months = [month[0:3] for month in calendar.month_name]
    if len(all_months) >12:
        return all_months[1:]
    return all_months


def found_month(date):
    """
    Search for single month not month range
    """
    date = date.strip()
    try:
        if int(date):
            return date
    except ValueError:
        ... 
    else:
        if '-' not in date:
            month_split =  date.split(' ')
            if len(month_split[0]) > 2 and month_split[0] in months():
                return date
            else:
                return False
        elif (len(date.split('-')[0]) > 2 and ' ' not in date.split('-')[-1]) and date.split('-')[0] in months():
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
    full_year = "Dec, " + year

    if str(sName) == str(year):
            df['DATE'] = full_year
    else:
        date  = re.findall(r'^\w*',sName)[0][0:3] + ", " + year
        df['DATE'] = monthyr_to_yrmonth(date)

    # convert date to date object
    df['DATE'] = pd.to_datetime(df['DATE'])
    return df