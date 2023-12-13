import shutil
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time


from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By


from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials


def html_schema(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table_data = []
    for container in soup.find_all("div", class_="container"):
        try:
            heading = container.find("h2")
            select_element_1 = container.find_all("select")[0]
            select_element_2 = container.find_all("select")[1]
            download_button = container.find("a", class_="btn btn-primary")
            download_id = download_button.get("id")
            for option_1 in select_element_1.find_all("option"):
                for option_2 in select_element_2.find_all("option"):
                    table_data.append([
                        select_element_1.get("data-set"),
                        select_element_1.get("id"),
                        option_1.text.strip(),
                        select_element_2.get("id"),
                        option_2.text.strip(),
                        download_id
                    ])
        except Exception as e:
            pass

    table_columns = ["data_set", "dtype_dropdown-1", "dtype_value", "geo_dropdown-2", "geo", 'download_id']
    table = pd.DataFrame(table_data, columns=table_columns)

    return table


def download_data(table, driver, downloads_dir: str, target_dir: str) -> None:
    data_ingested = []  # create an empty set to track ingested data

    for i, row in table.iterrows():
        data_set, dtype_value, geo = row[0], row[2], row[4]

        dtype_dropdown = row[1]
        geo_dropdown = row[3]
        download_link = row[5]

        try:
            dtype_drv = driver.find_element(By.ID, dtype_dropdown)
            geo_drv = driver.find_element(By.ID, geo_dropdown)

            dtype_drop_select = Select(dtype_drv)
            geo_drop_select = Select(geo_drv)

            # Print the tag name of each element found
            for dtype_option in dtype_drop_select.options:
                for geo_option in geo_drop_select.options:
                    if [dtype_option.text,
                        geo_option.text] not in data_ingested and 'Choose one...' not in geo_option.text:
                        print(dtype_option.text, geo_option.text)

                        # select option from Data Type
                        dtype_drop_select.select_by_visible_text(dtype_option.text)

                        # select option from Geography
                        geo_drop_select.select_by_visible_text(geo_option.text)
                        data_ingested.append([dtype_option.text, geo_option.text])

                        # click download link and wait for the download to complete
                        download_element = driver.find_element(By.ID, download_link)
                        download_element.click()

                        # wait for the download to complete
                        while True:
                            time.sleep(1)
                            if any(fname.endswith('.csv.part') for fname in os.listdir(downloads_dir)):
                                continue
                            elif any(fname.endswith('.crdownload') for fname in os.listdir(downloads_dir)):
                                continue
                            else:
                                break

                        # Move the downloaded file to the target directory
                        downloaded_file = max([downloads_dir + '/' + f for f in os.listdir(downloads_dir)],
                                              key=os.path.getctime)
                        shutil.move(downloaded_file, target_dir)

        except Exception as e:
            pass


def upload_files_to_drive(download_dir: str, folder_id: str, credentials_path: str) -> None:
    # Authenticate with Google Drive API
    creds = Credentials.from_service_account_file(credentials_path)
    drive_service = build('drive', 'v3', credentials=creds)

    # Get a list of all files in the downloads directory
    files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]

    # Upload each file to Google Drive
    for file_name in files:
        file_path = os.path.join(download_dir, file_name)
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        try:
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f'File ID: {file.get("id")} was successfully uploaded to Google Drive.')
            # Delete the file from the downloads directory
            os.remove(file_path)
            print(f'File {file_name} was successfully deleted from the downloads directory.')
        except HttpError as error:
            print(f'An error occurred while uploading file {file_name} to Google Drive: {error}')
