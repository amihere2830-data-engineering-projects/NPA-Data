from . import scrapper
import pandas as pd


class ExtractLinks:
   
    def __init__(self,url, main_div_id):
        """initialize main parameters for the class

        Args:
            main_div_id (string): It is the id for the div that contains Statistics, BDC, OMC and National pane
                at the npa main website.
            url (str, optional): A linke to npa main website. Defaults to 'http://www.npa.gov.gh/download-media/industry-data/downloads'.
        """
        self.url = url 
        self.main_div_id = main_div_id       


    def get_heading_links(self, list_of_separaters):
        """Retrieves titles under (Statistics, BDC, OMC and National) and their corresponding links

        Returns:
            dataframe: a dataframe of Heading and links to download them
        """
        scrapped = scrapper.scraping(self.url)
        try:
            headings = scrapped.find('div',id=str(self.main_div_id))
        
            title = [data.text.strip() for data in headings.find_all('td')]
            links = [a['href'] for a in headings.find_all('a', href=True) if a['href'][0:4]=='http']
            df = pd.DataFrame({'Heading':title, 'Link':links})
            different_dfs = [df[df['Heading'].str.match(i)].reset_index(drop=True) for i in list_of_separaters]
            
            return different_dfs

        except AttributeError:
            print("[Could not scrap information]")