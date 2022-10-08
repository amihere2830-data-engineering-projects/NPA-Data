import asyncio

async def create_object (ob:object, prop: tuple):
    obj = ob(prop)
    return obj

async def main():
    # Obtain properties needed to create objects                      
    nonHist = [get_books_details()[i] for i in NonHist_dw.values()]
    # indicativePr = [get_books_details()[i] for i in Ind_Prices.values()]
    # priceBU = [get_books_details()[i] for i in Prices_bu.values()]

    hist = [get_books_details()[Hist_dw['historical_trends']]]
    # pck objects and features to create various objects
    object_prop = [
        (NonHistoricalDownloads, nonHist),
        (HistoricalDownloads, hist),
        ]

    # Create objects
    objs = await asyncio.gather(*[create_object(ob, prop[i]) for ob, prop\
         in object_prop for i in range(len(prop))])

    # Extract all objects
    bdc_obj = objs[0]
    bidec_obj = objs[1]
    omc_obj = objs[2]

    # indicativePr_22 = objs[4]
    # indicativePr_21 = objs[5]
    # indicativePr_20 = objs[6]
    # indicativePr_19 = objs[7]
    # indicativePr_18 = objs[8]
    # indicativePr_17 = objs[9]

    # Objects
    nonHist_objs = [bdc_obj, bidec_obj, omc_obj]
    hist_obj = objs[3]
    # indicativePr_objs = [
    #     indicativePr_22,
    #     indicativePr_21,
    #     indicativePr_20,
    #     indicativePr_19,
    #     indicativePr_18,
    #     indicativePr_17
    #     ]
    
    # ------------------------------------------------------------
    # -------------                                   ------------
    #                    HistoricalDownloads
    # -----------------------------------------------------------
    # Provides data about products and their prices 
    # ------------------------------------------------------------
    tables_Hist = await hist_obj.findSheetTables()
    hist_data_paths = TablePaths(tables_Hist).getPaths()
    tables_Hist = get_final_data(tables_Hist, hist_data_paths)

    # ------------------------------------------------------------
    # -------------                                   ------------
    #                 NonHistoricalDownloads
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


if __name__=="__main__":
    from clean_data.loader import get_books_details
    from clean_data.search.find_data_paths import TablePaths
    from clean_data.downloads import (
        NonHistoricalDownloads,
        HistoricalDownloads
        )
    # from clean_data.price import IndicativePrices
    from clean_data.search.search import get_final_data

    NonHist_dw={
        'bdc': 1,
        'bidec':2,
        'omc': 3
    }
    Hist_dw={
        'historical_trends': 0
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
   
    asyncio.run(main())