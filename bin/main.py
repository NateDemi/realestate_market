import re
from multiprocessing import Pool
from data_processing import *

from process import *

indicators_file = 'zillow_indicators.csv'
inicators_track = 'zillow_indicators_ingest.csv'
api_keys = ['xuisyPUDscg1rq-HiMz7', 'prdogxsj8me2Mma8Xu5h']

max_calls_per_day = 50000
max_calls_per_10_seconds = 300
max_calls_per_10_minutes = 2000
concurrency_limit = 1

# get indicators
df_ind = incicators(api_keys)

# get regions by zip
df_regions = regions(api_keys)
df_regions_zip = df_regions[df_regions['region_type'] == 'zip']

df_regions_zip = df_regions_zip.copy()
df_regions_zip['region_str_len'] = df_regions_zip.apply(lambda x: len(x['region'].split(';')), axis=1)
df_regions_zip['zip'] = df_regions_zip.apply(lambda x: re.search('(\d{5})', x['region']).group(), axis=1)
df_regions_zip['state'] = df_regions_zip.apply(lambda x: check_state(x['region']), axis=1)
df_regions_zip['county'] = df_regions_zip.apply(lambda x: check_county(x['region']), axis=1)
df_regions_zip['city'] = df_regions_zip.apply(lambda x: check_city(x['region']), axis=1)
# df_regions_zip['metro'] = df_regions_zip.apply(lambda x: check_metro(x['region']), axis=1)

region_ids = df_regions_zip['region_id']
indicator_ids = df_ind['indicator_id']

# save indicator
df_ind.to_csv(indicators_file)

# select tracked indicator
indicators_df = pd.read_csv(inicators_track)
tracked_indicators = indicators_df[indicators_df['ingest'] == 'Y']['indicator_id']

zip_info = pd.read_csv('zip_info.csv')
print(zip_info.head())

df_filtered = df_regions_zip[df_regions_zip.isna().any(axis=1)]
# print(df_filtered.head(), len(df_filtered))


#
#
# if __name__ == '__main__':
#     api_keys = api_keys * len(tracked_indicators)
#     chunks = [tracked_indicators.iloc[i:i + 1000] for i in range(0, len(tracked_indicators), 1000)]
#     pool = Pool(processes=len(api_keys))
#     results = []
#     for i in range(len(chunks)):
#         results.append(pool.apply_async(get_data_chunk, [chunks[i]]))
#     pool.close()
#     pool.join()
#
#     df_data = pd.concat([r.get() for r in results], ignore_index=True)
#     print(df_data)


url = os.get_env("URL")
credentials_path = os.get_env("CREDENTIALS_PATH")
folder_id = os.get_env("FOLDER_ID")
files_dir= os.get_env("FILES_DIR")
downloads_dir = os.get_env("DOWNLOADS_DIR")



# Set up the Chrome webdriver
driver = webdriver.Chrome()

# driver.maximize_window() # For maximizing window
driver.implicitly_wait(20) # gives an implicit wait for 20 seconds

# Navigate to the Zillow Research website
driver.get(url)

# downloads file
download_data(html_schema(url), driver, downloads_dir, files_dir)

# uploads file to drive
upload_files_to_drive(files_dir, folder_id, credentials_path)

# closes driver
driver.quit()