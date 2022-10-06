import json
import os
from . books import Book

# -------------------------------------------------------------
#             load json file that has information
#             about the downloaded data
# -------------------------------------------------------------

try:
    with open('./info/download_hist.json') as json_file:
        download_data = json.load(json_file)
except  FileNotFoundError:
    # download_data = {}
    print("json file not found in info folder")


def get_books_details()->list[tuple]:
    """
    Get information about the books
    Each book requires two parameters
    """
    books = []
    for page in download_data['raw'].keys():
        for topic in download_data['raw'][page].keys():
            books.append((page, topic))
    return books

def get_sheets(obj: Book)->dict:
    """
    Load books of data objects from local directory
    """

    # load the raw data
    obj.load_sheets()

    curr_download_date = list(obj.sheets.keys())[0]
    books = obj.sheets[curr_download_date]
    return books
