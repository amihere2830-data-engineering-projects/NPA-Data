
import asyncio
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import urllib

load_dotenv(find_dotenv())


def get_market_data(df:pd.DataFrame)-> list[dict]:
    """
    Creates document structure of markets, {"name": "market_name",
    "description": full_name} for mongodb 
    """
    col_market = list(df.columns)[-1]
    market_lists = df[col_market].drop_duplicates().tolist()
    return  [{"name":mkt, "description": market_name_desc[mkt]} for mkt\
         in market_lists]

def get_company_details(df:pd.DataFrame, market_db_collection_name:list)->list[dict]:
        """
        Create dictionary of company names and their
        markets of operation
        """
        col_name_company = list(df.columns)[0]   # get column name for companies
        col_name_market = list(df.columns)[-1]   # get column name for markets
        companies = df.drop_duplicates(subset=[col_name_company, col_name_market])
        companies = list(companies.groupby([col_name_company, col_name_market]).\
            agg('sum').index)

        # get company names and their markets of operation
    
        company_markets = {}
        for comp_mkt in companies:
            if comp_mkt[0] in company_markets.keys():
                company_markets[comp_mkt[0]].append(market_db_collection_name[0].query('find_one', market_db_collection_name[1], comp_mkt[1]))
            else:
                company_markets[comp_mkt[0]] = [market_db_collection_name[0].query('find_one', market_db_collection_name[1], comp_mkt[1])]
        func = lambda  comp_name: {'name': comp_name, 'category': company_markets[comp_name]}
        return list(map(func, company_markets))

def get_url ():
    password = os.environ.get("MONGODB_USER_PWD")
    password = urllib.parse.quote(password) # Escape any symbol in the password
    url = f"mongodb+srv://npa:{password}@npa-data.79j7i.mongodb.net/?\
        retryWrites=true&w=majority"
    return url

async def create_object (ob:object, prop: tuple):
    obj = ob(prop)
    return obj

async def main():
    # Obtain properties needed to create objects                      
    nonHist = [get_books_details()[i] for i in NonHist_dw.values()]
    indicativePr = [get_books_details()[i] for i in Ind_Prices.values()]
    priceBU = [get_books_details()[i] for i in Prices_bu.values()]

    hist = [get_books_details()[Hist_dw['historical_trends']]]
    # pck objects and features to create various objects
    object_prop = [
        (NonHistoricalDownloads, nonHist),
        (HistoricalDownloads, hist),
        (IndicativePrices, indicativePr)
        ]

    # Create objects
    objs = await asyncio.gather(*[create_object(ob, prop[i]) for ob, prop\
         in object_prop for i in range(len(prop))])

    # Extract all objects
    bdc_obj = objs[0]
    bidec_obj = objs[1]
    omc_obj = objs[2]

    indicativePr_22 = objs[4]
    indicativePr_21 = objs[5]
    indicativePr_20 = objs[6]
    indicativePr_19 = objs[7]
    indicativePr_18 = objs[8]
    indicativePr_17 = objs[9]

    # Objects
    nonHist_objs = [bdc_obj, bidec_obj, omc_obj]
    hist_obj = objs[3]
    indicativePr_objs = [
        indicativePr_22,
        indicativePr_21,
        indicativePr_20,
        indicativePr_19,
        indicativePr_18,
        indicativePr_17
        ]
    
    # # ------------------------------------------------------------
    # # -------------                                   ------------
    # #                    HistoricalDownloads DATA
    # # -----------------------------------------------------------
    # # Provides data about products and their prices 
    # # ------------------------------------------------------------
    # tables_Hist = await hist_obj.findSheetTables()
    # hist_data_paths = TablePaths(tables_Hist).getPaths()
    # tables_Hist = get_final_data(tables_Hist, hist_data_paths)

    # ------------------------------------------------------------
    # -------------                                   ------------
    #                 NonHistoricalDownloads DATA
    # -----------------------------------------------------------
    # Provides data about companies and quantity of products sold
    # ------------------------------------------------------------
    tables_nonHist = await asyncio.gather(*[nonHist_objs[i].\
        findSheetTables() for i in range (len(nonHist_objs))])
    # BDC
    tables_bdc = tables_nonHist[0]
    tables_bdc = get_final_data(tables_bdc, TablePaths(tables_bdc)\
        .getPaths())
    # BIDEC
    tables_bidec = tables_nonHist[1]
    tables_bidec = get_final_data(tables_bidec, TablePaths(tables_bidec)\
        .getPaths())
    # OMC
    tables_omc = tables_nonHist[2]
    tables_omc = get_final_data(tables_omc, TablePaths(tables_omc)\
        .getPaths())
    
    print("Done!!!!!!")

    tables = [*tables_bdc, *tables_bidec, *tables_omc]

    tables_concat = pd.concat(tables, ignore_index=True)
    

    
    # -----------------------------------------------------
    # ---------- Create Mongodb client connection ---------
    # -----------------------------------------------------

    client = MongoClient(get_url())


    db_name = 'npa_db'
    
    
    # ---------Use market info to initialise database----------
    market_db_data = get_market_data(tables_concat)
    collection_name_mkt = 'markets'

    market = NPAdb(client, db_name)
    # insert documents
    market.insert_document(market_db_data, collection_name_mkt)

    # -------Use company info to initialise database----------
    market_coll = [market, collection_name_mkt]
    company_db_data = get_company_details(tables_concat, market_coll)
    collection_name_comp = 'companies'

    company = NPAdb(client, db_name)
    # insert documents
    company.insert_document(company_db_data, collection_name_comp)


    client.close()













if __name__=="__main__":
    from clean_data.loader import get_books_details
    from clean_data.search.find_data_paths import TablePaths
    from clean_data.downloads import (
        NonHistoricalDownloads,
        HistoricalDownloads
        )
    from clean_data.price import IndicativePrices
    from clean_data.search.search import get_final_data
    from mongo.db import NPAdb

    Hist_dw={
        'historical_trends': 0
    }
    NonHist_dw={
        'bdc': 1,
        'bidec':2,
        'omc': 3
    }
    
    Ind_Prices={
        'indicative_price_2022': 4,
        'indicative_price_2021': 5,
        'indicative_price_2020': 6,
        'indicative_price_2019': 7,
        'indicative_price_2018': 8,
        'indicative_price_2017': 9,
    }
    Prices_bu={
        'price_build_up_2022': 10,
        'price_build_up_2021': 11,
        'price_build_up_2020': 12,
        'price_build_up_2019': 13,
        'price_build_up_2018': 14
    }

    market_name_desc={
        'OMC': 'Oil Marketing Company',
        'BIDECs': 'Bulk Import Distribution and Export Company',
        'BDC': 'Bulk Distribution Company'
    }
   
    asyncio.run(main())
