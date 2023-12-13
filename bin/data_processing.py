import quandl
# import pgeocode

# import numpy as np
import pandas as pd
# import time
# import threading
# from multiprocessing import Pool
#
# # logging warnings and errors
import logging
# import sys
# from io import StringIO
import numpy as np

logging.basicConfig(filename='log_file.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filemode='a')
logger = logging.getLogger()

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]


def check_state(search_str):
    search_str_list = [x.strip() for x in search_str.split(';')]
    for x in search_str_list:
        if x in states:
            return x


def check_county(search_str):
    search_str_list = [x.strip() for x in search_str.split(';')]
    for x in search_str_list:
        if 'county' in x.lower():
            return x


def check_city(search_str):
    search_str_list = [x.strip() for x in search_str.split(';')]
    if len(search_str_list) == 1:
        return np.nan
    if 'county' not in search_str_list[-1].lower():
        return search_str_list[-1]


def check_metro(search_str):
    search_str_list = [x.strip() for x in search_str.split(';')]
    if len(search_str_list) <= 3:
        return np.nan
    if 'county' not in search_str_list[2].lower():
        return search_str_list[2]

    # get indicators


def incicators(api_keys):
    try:
        for api_key in api_keys:
            quandl.ApiConfig.api_key = api_key
            return quandl.get_table("ZILLOW/INDICATORS", paginate=True)
    except Exception as e:
        logger.error(f"Failed to retrieve data using API key '{api_key}': {e}")


# get regions by zip
def regions(api_keys):
    try:
        for api_key in api_keys:
            quandl.ApiConfig.api_key = api_key
            return quandl.get_table("ZILLOW/REGIONS", paginate=True)
    except Exception as e:
        logger.error(f"Failed to retrieve data using API key '{api_key}': {e}")


def quandl_data(indicator_id, region_id=None, api_key=None):
    try:
        quandl.ApiConfig.api_key = api_key
        return quandl.get_table('ZILLOW/DATA', indicator_id=indicator_id, region_id=region_id, paginate=True)
    except Exception as e:
        logger.error(f"Failed to retrieve data using API key '{api_key}': {e}")


def get_data_chunk(tracked_indicators_chunk):
    df_data = pd.DataFrame(columns=['indicator_id', 'region_id', 'date', 'value'])

    for _, row in tracked_indicators_chunk.iterrows():
        ind = row['indicator_id']
        region = row['region_id']

        try:
            data = quandl_data(ind, region)
            if not data.empty:
                df_data = pd.concat([df_data, data], ignore_index=True)
                print(f'Indicator: {ind} - Region: {region} - Successfully Ingested')

        except Exception as e:
            print(f"Error in retrieving data for Indicator {ind} - Region {region}: {e}")
    return df_data