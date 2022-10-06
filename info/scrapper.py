import requests
from bs4 import BeautifulSoup
import pandas as pd
from extract import create_directory_if_not_exists



def scraping(my_url):
    """

    """

    url = my_url
    try:
        response = requests.get(url)
        html = response.content
        return BeautifulSoup(html, 'html.parser')
    except requests.exceptions.ConnectionError:
        print("[Could not establish connections]")


def get_info_of_topic(pages, all_info):
    """
    Retrieves information about files in df form
    """
    
    pages_df = {}
    try:
        for page in pages:
            topics = all_info['raw'][page].keys()
            # print(topics)
            topic_dict = {}
            for top in topics:
                downloaded_at = all_info['raw'][page][top].keys()

                topic_df = []
                for i in downloaded_at:
                    data = all_info['raw'][page][top][i]
                    df = pd.DataFrame({i:data})
                    if len(topic_df)==0:
                        topic_df.append(df)
                    else:
                        topic_df[0][i]=data
                topic_dict[top] = topic_df
            pages_df[page] = topic_dict
        return pages_df
    except KeyError:
        print(f'[{all_info} does not exist]')

def save_history(csv_filename,df_new):
    try:
        with open(csv_filename, 'r') as file:
            df_hist = pd.read_csv(file)
            if df_hist.columns.tolist() == df_new.columns.tolist():
                print("No new data")
            elif df_new.columns.tolist() not in df_hist.columns.tolist():
                print("New data is available")
                new_cols = df_hist.columns.tolist().append(df_new.columns.tolist()[0])
                df_hist[df_new.columns.tolist()[0]] = df_new[df_new.columns.tolist()[0]]
            df_hist.to_csv(csv_filename, index=False)
    except FileNotFoundError:
        create_directory_if_not_exists(csv_filename)
        df_new.to_csv(csv_filename, index=False)

def check_current_data():
    ...