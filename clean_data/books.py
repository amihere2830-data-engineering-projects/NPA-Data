import re
import pandas as pd
import os

base_path = os.path.abspath(__file__ + "/../../")

class Book:
    books_paths = {}

    def __init__(self, page_details:tuple) -> None:
        self.page_details = page_details
        self.download_date:str = ""
        self.sheets:dict = {}

    def set_full_paths(self):
        """Get full paths of books for every download date
        """
        page_name_topic = f"{self.page_details[0]}/{self.page_details[1]}"
        data_path = f"{base_path}/data/src/raw/{page_name_topic}"
        dowload_date_path = [f"{data_path}/{subFolder}" for subFolder in os.listdir(data_path)]

        for i in dowload_date_path:
            download_date = i.split('/')[-1]
            self.books_paths[download_date] = [f"{i}/{j}" for j in os.listdir(i)]
        
    def get_filenames(self):
        self.download_date = sorted(self.books_paths.keys())[-1]
        
        return self.books_paths[self.download_date]

    def load_sheets(self):
        """Load the books from source
        and get their corresponding sheets
        """
        # set path of sheets
        self.set_full_paths()

        

        final_sheets = {}
        # print(len(self.books_paths[self.download_date]))

        for book in self.get_filenames():
            # Extract the year from url name
            get_year = lambda x: re.findall(r'2\d{3}',x)[0]

            bk, p_yr = (pd.ExcelFile(book), get_year(book.split('/')[-1]))

            final_sheets[p_yr] = {sheetName:bk.parse(sheetName) for sheetName in bk.sheet_names}
            self.sheets[self.download_date] = final_sheets
