from info import npa
import os
from datetime import date
import json
import re


download_hist ={}

def list_is_int(lst):
    try:
        if int(lst):
            return True
    except ValueError:
        return False

def get_date(splitted):
    try:
        if re.findall(r'(\w*.\w*)|(\w*)', (splitted[0]+splitted[1]).lower())[0][0]\
            in ['pricebuild-up','pricebuild','indicativeprices']:
            try:
                some_date = [(True,j[0:4]) for j in splitted if (list_is_int(j) and len(j)>=4)][0]
            except:
                some_date = (False,"could'nt determine")

            return some_date
        else:
            some_date = (False,"could'nt determine")
            return some_date
    except IndexError:
            some_date = (False,"could'nt determine")
            return some_date

def initialisePaths(b_path,head,link):

    splitted = head.split(' ')

    if get_date(splitted)[0]:
        head = get_date(splitted)[1]
    else:
        head = '_'.join(head.split(' ')[0:2])
    data_path = f"Raw/{b_path}/{head}/downloaded_at={date_today}/{link}"

    #Create a dictionary to hold
    #Information about paths of files
    dd1 = f'downloaded_at={date_today}'
    dd2 = {head:dd1}
    dd3 = {b_path:dd2}
    if len(download_hist)==0:
        download_hist['raw'] = dd3
        
    else:
        if b_path not in download_hist['raw'].keys():
            download_hist['raw'][b_path] = dd2
        if head not in download_hist['raw'][b_path].keys():
            download_hist['raw'][b_path][head] = dd1
    return data_path

# Create a directory at the `path` passed as an argument
def create_directory_if_not_exists(path):
    """
    Create a new directory if it doesn't exists
    """

    os.makedirs(os.path.dirname(path), exist_ok=True)



# Write the file obtained to the specified directory
def download_snapshot(b_path, df):
    """ 
    Download the new dataset from the source using its url
    """
    topic_urls = {}  # get all the current topics and their urls

    for item in df.itertuples(index = True, name ='Pandas'):        
        heading = getattr(item, "Heading")
        url =  str(getattr(item, "Link"))
        data_pah = initialisePaths(b_path,heading, url)
        
        top = data_pah.split('/')[2]
        if top in topic_urls.keys():
            topic_urls[top].append(f"http:{data_pah.split('http:')[-1]}")
        else:
            topic_urls[top] = [f"http:{data_pah.split('http:')[-1]}"]

    for topic in topic_urls.keys():        
        url_ = topic_urls[topic]
        b = data_pah.split('/')[3:4]
        download_hist["raw"][b_path][topic] = {b[0]:url_}
       


def download_files(url,div_id,back_path, topcs):
    obj = npa.ExtractLinks(url, div_id)   #main_div_id : id from the npa website
    try:
        for top in obj.get_heading_links(topcs):
            download_snapshot(back_path,top) #Download data 
    except TypeError:
        print("[Can not download file]")


def new_json_file(fname:str,json_data:dict)->None:
    """Saves a new data the download history to a json file
    for non existing json file
    """
    print('[Saving Fresh Data.........]')
    with open(fname, "w+") as outfile:
        json.dump(json_data, outfile, indent=4)


def save_json_file(fname)->None:
    """Updates existing json file with new files downloaded at
    a particular date. 
    """
    try:
        with open(fname, "r+") as file:
            if os.path.getsize(fname) != 0:
                print('[Found existing file]')
                file_data = json.load(file)
                for page in download_hist['raw'].keys():
                    for topic in download_hist['raw'][page].keys():

                        # pick previous urls and compare with new urls and save the new one
                        previous_download_date = list(file_data['raw'][page][topic].keys())[-1]
                        old_urls = file_data['raw'][page][topic][previous_download_date]
                        # print(list(download_hist['raw'][page][topic].keys())[-1])
                        current_download_date = list(download_hist['raw'][page][topic].keys())[-1]
                        current_url = download_hist['raw'][page][topic][current_download_date]
                        new_url = list(set(current_url)-set(old_urls))

                        new_u = current_url[-1]
                        # print(new_u)
                        # if new_u not in old_urls:
                        #     ...
                        # else:
                        #     print(f'{topic}:{new_u}')
                        if new_u not in old_urls:
                            # remove all old downloads and replace with only the current download
                            download_hist['raw'][page][topic][current_download_date] = new_url
                            file_data['raw'][page][topic].update(download_hist['raw'][page][topic])#
                            file.seek(0)
                            json.dump(file_data, file, indent=4)
                            print(f'[New files are added: {topic}]')
                            print(new_url)
                        else:
                            print('[No new files are added]')
                            # break
            else:
                new_json_file(fname,download_hist)  
    except FileNotFoundError:
        new_json_file(fname,download_hist)

if __name__ == '__main__':
    date_today = str(date.today())
    base_path = os.path.abspath(__file__ + "/../../")

    #--------1.Download files at downloads--------#
    url_dw='http://www.npa.gov.gh/download-media/industry-data/downloads'
    back_path_dw= 'downloads'
    tops_dw = [r'Historical',r'BDC|BIDECs',r'OMC']
    main_div_id_dw="column-id-1624921182338"
    download_files(url_dw,main_div_id_dw,back_path_dw, tops_dw)


    #--------2. Download files at Indicative Prices------#
    url_ip='http://www.npa.gov.gh/download-media/industry-data/indicative-prices'
    back_path_ip= 'indicative prices'
    # 2022,2021
    tops_22_21 = [r'.* 2022',r'.* 2021']
    main_div_id_22_21="column-id-1624922483776"
    download_files(url_ip,main_div_id_22_21,back_path_ip, tops_22_21)
    # 2020,2019
    tops_20_19 = [r'.* 2020',r'.* 2019']
    main_div_id_20_19="column-id-1624922483777"
    download_files(url_ip,main_div_id_20_19,back_path_ip, tops_20_19)
    # 2018,2017
    tops_18_17 = [r'.* 2018',r'.* 2017']
    main_div_id_18_17="column-id-1625353518965"
    download_files(url_ip,main_div_id_18_17,back_path_ip, tops_18_17)


    #--------3. Download files at Price Build-Up--------#
    url_bp = "http://www.npa.gov.gh/download-media/industry-data/price-build-up"
    back_path_pb = 'Price Build-Up'
    # 2022,2021
    tops_pb_22_21 = [r'.* 2022',r'.* 2021']
    main_div_id="column-id-1624916953540"
    download_files(url_bp,main_div_id,back_path_pb, tops_pb_22_21)
    # 2020,2018,2018
    tops_pb_20_18 = [r'.* 2020',r'.* 2019',r'.* 2018']
    main_div_id_20_18="column-id-1624916953542"
    download_files(url_bp,main_div_id_20_18,back_path_pb, tops_pb_20_18)


    # # #Save download_hist as json
    jfile = f"./info/download_hist.json"
    save_json_file(jfile)