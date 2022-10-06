from clean_data.loader import get_sheets
import pandas as pd
import re
from  clean_data.books import Book
import asyncio
import warnings

warnings.filterwarnings("ignore")

class Price (Book):
    def initialEmptyColumns(self,df)->list:
        """Search for columns whose rows are all empty"""
        empty_cols = []
        for nm, i in enumerate(df.columns):
            func = lambda x: df.iloc[:,x:x+1]
            try:
                if list(func(nm).sum()<len(func(nm)))[0]:
                    empty_cols.append(nm)
                if list(func(nm+1).sum()<len(func(nm)))[0] and\
                     list(func(nm+1).sum())[0]>10:
                    break
            except IndexError:
                return empty_cols
        return empty_cols        


class IndicativePrices(Price):
    PRODUCTS = []

    def create_product_tables(self,df:pd\
        , datte:str)->dict:
        """Finds table in the 'Indicative 
        Prices' sheets"""

        start_col = self.initialEmptyColumns(df)[-1]
        table = df.iloc[:,start_col+1:]

        # Identify where headings are
        #Extract headings and required table values

        idx = []        #gets starting index and last index for final table
        for item in table.itertuples(index = True, name ='Pandas'):
            if str(item._1)!="nan" and str(item._2)!='nan' and str(item._3)!='nan':
                idx.append(item.Index)

        table = table[idx[0]:idx[-1]].reset_index(drop=True) #Eliminate top rows
        columns =  table[0:1].values[0]       #gets columns for final table
        final_table = table[1:]               #get final rows
        final_table.reset_index(drop=True,inplace=True)

        #rename columns with found columns
        final_table.rename(columns={table.columns[old]:nw for old\
                                    , nw in enumerate(columns)}, inplace=True) 

        # Eliminate No. and empty columns
        for col in final_table.columns:
            try:
                if re.findall(r'NO|No.|No|no.|no.|nan',str(col)) or\
                sum(list(final_table[col].isnull()))>0 :
                    final_table.drop(columns=[col], inplace=True)
            except TypeError:
                ...
        # Add dates
        final_table['Publish_Date'] = datte
        final_table['Publish_Date'] = pd.to_datetime(final_table['Publish_Date']\
            ,dayfirst=True)
        return final_table



    async def findSheetTables(self)-> dict:
        """Provides tales for Indicative prices. There are
        two keys 'BIDEC Ex-Refinery Prices' and 'OMCs and LPGMCs Ex-Pump Prices'
        Each key has list of tables representing dates of publication"""     
   

        sht_tables = {}
        func = lambda x: re.findall(r'-| ',x)

        # load the raw data
        books = get_sheets(self)
        # counter = 0
        for file_ in self.get_filenames():
            # counter+=1
            # print(counter)
            for p_year in books:
                if len(books)!=0:
                    ini = {}
                    
                    for sname, df in books[p_year].items():
                        
                        if '-' in func(file_):
                            date_ = file_.split('-')[-1][0:-5] #pick date from file name
                        else:
                            date_ = file_.split(' ')[-1][0:-5] #pick date from file name
                        if sname in ini.keys():
                            ini[f'{sname}'].append(self.create_product_tables(df\
                                ,date_))
                        else:
                            ini[f'{sname}'] = [self.create_product_tables(df\
                                ,date_)]
                    sht_tables[p_year] = ini
        self.PRODUCTS = list(sht_tables.keys()) 
        return sht_tables