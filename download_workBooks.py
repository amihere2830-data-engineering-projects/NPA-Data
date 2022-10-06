from clean_data.loader import get_books_details, download_data
from clean_data.page import Page
import os
import asyncio
import time
import os
import requests




class WorkBook:

    data_src = None

    def __init__(self, page:object, downloadInfo, raw_files_path=""):
        self.pName = page.pName
        self.topic = page.topic
        # self.files = page.currentFiles(downloadInfo)
        self.files = page.allFiles(downloadInfo)
        self.raw_files_path = raw_files_path
        
        self.sheetName:dict = {}

    
    def create_directory_if_not_exists(self, path):
        """
        Create a new directory if it doesn't exists
        """
        # print(os.path.dirname(self.raw_files_path))
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def formatURL(self,ur):
        """"Formats the url link by replacing spaces with %20
        """
        url='http://www.npa.gov.gh/Data/Documents/'
        for i in ur[37:]:
            if i == ' ':
                url = url+'%20'
            else: 
                url = url+i
        return url

    async def get_tasks(self, url):
        """Downloads the workBook from the given url"""
        return (url, requests.get(self.formatURL(url)))

    async def save_book(self):
        """Download and save workbooks into data directory
        """
                
        responses = await asyncio.gather(*[self.get_tasks(ur) for ur in self.files])

        results = [res for res in responses]

        # Create directory for files
        self.create_directory_if_not_exists(self.raw_files_path)

        for ur_, res in results:
            self.data_src = f"{self.raw_files_path}{ur_.split('/')[-1]}"
            files = os.listdir(self.raw_files_path)

            if len(files) == len(self.raw_files_path):
                ...

            else:
                try:
                    with open(self.data_src, "wb") as source_ppr:                           
                        source_ppr.write(res.content)

                except:
                    print("couldn't open folder")
                    pass

async def main():
    # create page objects
    objs = [Page(item[0], item[1]) for item in get_books_details()]

    # Create workbooks
    wbs_obj = []
    for obj in objs:
        # Retrieve raw data source
        if download_data:
            # reset file_path to empty list to avoid items from 
            # different objects
            obj.file_path= []
            # set file paths
            obj.allFiles(download_data)
            # Create full file path
            raw_data_src =  '/'.join([base_path,*obj.file_path])
        # create workbooks
        wbs_obj.append(WorkBook(obj,download_data,raw_data_src))

    # Download all workbooks
    st= time.time()

    # save all workbooks to data directory
    for wb in wbs_obj:
        await wb.save_book()
        
    print(time.time() - st)



if __name__=="__main__":

    base_path = os.path.abspath(__file__ + "/../")

    asyncio.run(main())