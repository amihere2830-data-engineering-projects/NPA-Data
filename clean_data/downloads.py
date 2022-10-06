import os
# from pyclbr import Function
from  clean_data.books import Book
# from typing import Any
import pandas as pd
from collections import OrderedDict
import re
import asyncio
import time
# from download_workBooks import  get_books_details
from . loader import get_sheets


class Downloads (Book):
    COMPANIES = {"companies":[], "date":[]}
    UNIQUE_COMPANIES:pd.DataFrame

    async def formatTable(self,sName, df, yr): 
        data =  await  self.getFinalTableList(sName,df,yr) 

        # format company names
        for i in  data.columns:
            if type(i) == str:
                company_col = i
                break
            else:
                continue

        # print(sName)
        # print(type(company_col))
        date_col = data.columns[-1] 

        def format_name(name):
            name = name.strip()
            if ' ' in name:
                # Capitalise each word in  a company's name
                name = name.title()
            # remove dot at the end of name
            try:
                # name = re.findall(r'^[^\.]$',name)[0]
                ...
            except IndexError:
                ...
            # rmove ltd from company name to help remove duplicates
            # A name could be written at times with ltd at times without
            name = " ".join([i for i in name.split(' ') if i not in ['Limited', 'Ltd']])
            
            return name
        # print(company_col)

        data[company_col] = data[company_col].apply(lambda comp_name: format_name(comp_name))

        # Add all companies to COMPANIES
        self.COMPANIES["companies"] = [*self.COMPANIES["companies"], *list(data[company_col])]
        self.COMPANIES["date"] = [*self.COMPANIES["date"], *list(data[date_col])]

        return  {sName:data}
    
    async def findFormattedTables(self,books,year):
        st = time.time()
        formated = {}
        sheets = await asyncio.gather(*[self.formatTable(sName, df,year) for sName, df in books[year].items()])
        for sheet in sheets:
            for sn, df in sheet.items():
                formated[sn] = df
        print(f'[{self.page_details[1]} - Formating]: [{time.time()-st} secons]')
        return formated

    async def findSheetTables(self):
        """Provides final tables for download pages
        funcs contains worbook's getAllTables and downloads subcalss' getFinalTableList"""

        final_tables = {self.page_details[1].split("_")[0]:{}}
        # load the raw data
        books = get_sheets(self)
        sht_tables =  await asyncio.gather(*[self.findFormattedTables(books,p_yr) for p_yr in books])
        # get list of companies and store in COMPANIES variable
        self.UNIQUE_COMPANIES = pd.DataFrame(self.COMPANIES).drop_duplicates(subset='companies')
        def get_final_tables(dic):
            final_tables[self.page_details[1].split("_")[0]][dic[0]] = dic[1]
        func = lambda tab: map(get_final_tables, tab.items())
        # Populate final_tables
        map(func, sht_tables)
    
        return final_tables


#-------------------------------------------------------------------------------------------------------

class HistoricalDownloads(Downloads):
    PRODUCTS_IN_SHEET_NAME={}
    

    async def getFinalTableList(self,sName,dff,year):
        """Find the table of historical trends from 1/1/2011 todate"""
        products,p_row = self.getProducts(dff,sName)

        df_values = dff.values[p_row+3:]
        init_col = dff.values[p_row+1][2:]
        rate_str = dff.values[p_row+2][1:]
        
        columns = ['effect_date',f'exchange_rate_{rate_str[0]}']
        try:
            for num,i in enumerate(init_col):
                columns.append(i.lower()+'_'+rate_str[num+1])
        except TypeError:
            ...
        
        dff = pd.DataFrame(df_values,columns=columns)
        return self.createProductsSubtables(products, sName,columns,dff,year)


    def getProducts(self,df,sname: str):
        """Retrieves tables of product"""
        if sname=="Export Products":
            p_row=1
        elif sname in ["Domestic Pdts (Jul 2015 - date)"\
            ,"Domestic Pdts (1989-Jul '07)"]:
            p_row=3
        elif sname=="Domestic Pdts (Aug '07-Jun '15)":
            p_row=4
        products = df.values[p_row][2:]
        products= [i for i in products if str(i)!='nan']
        return products,p_row


    def createProductsSubtables(self, products, sName, columns,df,year)->list:
        """Creates sub tables containing prices
        and duties of various products"""

        product_tables = {}
        prod_id=0
        column_nm = 2
        cnt = 2
        for col_name in columns[2:]:
            cnt+=1
            if col_name[0:7] in ['changes','change']:
                product_tables[products[prod_id]] =  (column_nm,cnt)
                column_nm = cnt       
                prod_id +=1
    
        
        sub_tabs = {}       # Holds Sub tables for various products
        for prod in products:
            st,endd = product_tables[prod][0],product_tables[prod][1]
            sub_tab = df.iloc[:,st:endd]
            sub_tab = pd.concat([df.iloc[:,0:2],sub_tab],axis=1)
            
            #Look for for the index of last row (idx_end)
            for item in sub_tab.itertuples(index = True, name ='Pandas'):
                if str(item.effect_date)=="NaT" and str(item._2)=='nan':
                    idx_end = item.Index
                    break
                elif str(item.effect_date)=="nan" and str(item._2)=='nan':
                    idx_end = item.Index
                    break
                else:
                    idx_end = item.Index+1
            sub_tab = sub_tab[0:idx_end]  #Get final sub table      
            sub_tab['product'] = prod     #Add product name 

            sub_tab['DATE'] = sName

            

            sub_tabs[prod] = sub_tab
            self.PRODUCTS_IN_SHEET_NAME[sName] = list(sub_tabs.keys())
        return sub_tabs

#-------------------------------------------------------------------------------------------------------

class NonHistoricalDownloads(Downloads):
    def findConvertedTable(self,df,sname,yr):
        """Find the indices of the start and end of table
        where conversion factors were applied on"""
        
        def findIdx(item):
            """Find first index of the table"""
            if ('COMPANY' in item or 'Company' in item or 'company' in item) and item.Index>0:
                return item.Index
        
        def findIdxEnd(item):
            """Find last index of the table"""
            try:
                if item.COMPANY.strip(' ') == 'TOTAL' or item.COMPANY ==\
                    'Total' or item.COMPANY == 'total':
                    return item.Index
            except AttributeError:
                ...

        idx = list(map(findIdx,df.itertuples(index = True, name ='Pandas')))
        idx = list(OrderedDict.fromkeys(idx))[1:]
        idx_end = list(map(findIdxEnd, df.itertuples(index = True, name ='Pandas')))
        idx_end = list(OrderedDict.fromkeys(idx_end))[1:]
        df = df[idx[0]+1:idx_end[1]].reset_index(drop=True)

        df['DATE'] = sname

        return df


    async def getFinalTableList(self,sname,dff,yr):
        """Get the final extracted table that's converted using the 
        conversion factors"""
        comp = ['COMPANY', 'Company','company']
            
        for i in comp:
            if i in list(dff.values[0]):
                cols = list(dff.values[0])
                values = dff.values[1:]
            elif i in list(dff.values[1]):
                cols = list(dff.values[1])
                values = dff.values[2:]                
        df = pd.DataFrame(values, columns=cols)
        # columns = list(df.columns)

        for col in df.columns:
            if col == 'Company' or col=='company':
                df.rename(columns={col:'COMPANY'},inplace=True)
            try:
                if col in re.findall(r'NO.|no.|NO|no|No',col):
                    df.drop(columns=[col],inplace=True)
            except TypeError:
                ...
        if str(df.columns[-1]) == 'nan':
            df.rename(columns={col:'ALL PRODUCTS'},inplace=True)

        return self.findConvertedTable(df,sname,yr)