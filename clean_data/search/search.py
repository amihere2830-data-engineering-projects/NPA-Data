from datetime import datetime
import re
import pandas as pd
import calendar


def get_final_data (table:dict, dict_key_info:list)->list:
        """
        Use the key information from 'TablePaths' of the dictonary to retrieve 
        final tables
        """
        
        all_data = []
        for key_list in dict_key_info:
            first_key = key_list[0]
            sub_keys = key_list[1:]
            sub_data = table[first_key]
            # Loop through until the last data (table)
            # is obtained
            for ky in sub_keys:
                sub_data = sub_data[ky]
            all_data.append(sub_data)
        return all_data

def months():
    """
    Get list of months
    """
    all_months = [month[0:3] for month in calendar.month_name]
    return all_months


def found_month(date):
    """
    Search for single month not month range
    """
    try:
        if int(date):
            return date
    except ValueError:
        ...
    if '-' not in date:
        month_split =  date.split(' ')
        if len(month_split[0]) > 2 and month_split[0] in months():
            return date
        else:
            return False
    elif '-' in date:
        month_split = date.split('-')
        if len(month_split[0]) > 2 and ' ' not in month_split[-1]:
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

def rename_petroleum_products(df:pd.DataFrame,column:str, old_new_names:list[tuple])->pd.DataFrame:
    """
    Rename products in a dataframe by replacing using (old_name, new_name)
    and the dataframe and the column name for the products
    """
    for old, nw in old_new_names:
        df[column] = df[column].apply(lambda x: nw if str(x).upper() == old else x)
    return df

def create_market_category(df:pd.DataFrame, col:str, values:list[str]):
    df[col] = values
    return df