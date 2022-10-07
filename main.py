import asyncio
import re
import time
from clean_data.loader import get_books_details
from clean_data.downloads import (
    NonHistoricalDownloads,
    HistoricalDownloads
    )
from clean_data.price import IndicativePrices


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
    objs = await asyncio.gather(*[create_object(ob, prop[i]) for ob, prop in object_prop for i in range(len(prop))])

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
    
    # priceBU_22 = IndicativePrices(priceBU[0])
    # priceBU_21 = IndicativePrices(priceBU[1])
    # priceBU_20 = IndicativePrices(priceBU[2])
    # priceBU_19 = IndicativePrices(priceBU[3])
    # priceBU_18 = IndicativePrices(priceBU[4])


    st = time.time()

    # tables_nonHist = await asyncio.gather(*[nonHist_objs[i].findSheetTables() for i in range (len(nonHist_objs))])
    # tables_Hist = await hist_obj.findSheetTables()


    print(time.time()-st)

if __name__=="__main__":

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