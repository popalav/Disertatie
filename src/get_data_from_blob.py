import os
from collections import namedtuple
from typing import List
from pathlib import Path
import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient
from utilities import Actions

N_STR = 'BlobEndpoint=https://disertatiestorageaccount.blob.core.windows.net/;QueueEndpoint=https://disertatiestorageaccount.queue.core.windows.net/;FileEndpoint=https://disertatiestorageaccount.file.core.windows.net/;TableEndpoint=https://disertatiestorageaccount.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-03-18T07:37:21Z&st=2022-03-17T23:37:21Z&spr=https&sig=uvHB8JM97JDU6%2BPSVGQheKH5PtAuTlyStJYyjgh%2B9I4%3D'
BLOB_CONN_STR = 'BlobEndpoint=https://disertatiestorageaccount.blob.core.windows.net/;QueueEndpoint=https://disertatiestorageaccount.queue.core.windows.net/;FileEndpoint=https://disertatiestorageaccount.file.core.windows.net/;TableEndpoint=https://disertatiestorageaccount.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-10-08T13:59:02Z&st=2022-04-13T05:59:02Z&spr=https,http&sig=oIjqHMLHKmZRVNhbSze4PzBtTo6F%2BZHZ01plxAucRR4%3D'
CONTAINER = ContainerClient.from_connection_string(conn_str=BLOB_CONN_STR, container_name="disertatie")

Timestamp = namedtuple("Timestamp", "minutes hours days month year")

def get_all_blobs():
    """ Returns an iterator with all blobs from a container """
    blob_list = CONTAINER.list_blobs()
    return blob_list

"""
for blob in blob_list:
    # get first level folder names
    blob_main_folder = blob.name.split('/')[1]
    # get second order folder, year
    blob_year = blob.name.split('/')[2]
    # get third order folder, month
    blob_month = blob.name.split('/')[3]
    # get fourth order folder, day
    blob_day = blob.name.split('/')[4]
    # get fifth order folder, hour
    blob_hour = blob.name.split('/')[5]
    # get fourth order folder, minute
    blob_minute = blob.name.split('/')[6]
    print(blob.name + '\n')
"""
# Download a blob

blob = BlobClient.from_connection_string(conn_str=BLOB_CONN_STR,
                                         container_name="disertatie",
                                         blob_name="fb1231ec-5b6b-476b-9751-71ae87ed1766/14/2022/03/18/00/22/wyta6lqqsiw6i")

with open("./BlockDestination.json", "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)

def get_first_order_folder(blob_path: str) -> str:
    """ Returns first order folder for a blob path """
    return blob_path.split('/')[2]

def download_blob_by_name(blob_name):
    # Download a blob
    blob = BlobClient.from_connection_string(conn_str=BLOB_CONN_STR,
                                             container_name="disertatie",
                                             blob_name=blob_name)
    file_name = blob_name.split('/')
    file_name = '.'.join(map(str, file_name))
    with open(f"{file_name}.txt", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

def download_blobs_by_month(month):
    """ Creates a folder with month name and downloads all blobs"""
    newpath = f'.\\src\\{month}'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    os.chdir(newpath)
    all_blobs = get_all_blobs()
    for blob in all_blobs:
        # get third order folder, month
        blob_month = blob.name.split('/')[3]
        if blob_month == month:
            download_blob_by_name(blob.name)

def download_blobs_by_day(month:str, day:str, master_folder:str):
    """ Creates a folder with day name inside month folder and downloads all blobs """
    # Master folder is the top level folder which records data for each person, 
    # e.g mine is 29
    current_path = Path(__file__).parent.resolve()
    newpath = f'{current_path}\\{month}\\{day}'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    os.chdir(newpath)
    all_blobs = get_all_blobs()
    for blob in all_blobs:
        # filter blobs by second folder name
        if blob.name.split('/')[1] == master_folder:
        # get third order folder, month
            blob_month = blob.name.split('/')[3]
            if blob_month == month:
                download_blob_by_name(blob.name)

def donwnload_blob_by_time(month: str, day: str, hour: str, minute: str) -> None:
    """ Downloads blob at a specified date and time"""
    # Download a blob
    blob = BlobClient.from_connection_string(conn_str=BLOB_CONN_STR,
                                             container_name="disertatie",
                                             blob_name=f"fb1231ec-5b6b-476b-9751-71ae87ed1766/29/2022/{month}/{day}/{hour}/{minute}/wyta6lqqsiw6i")
    with open(f"./Blob_{month}_{day}_{hour}_{minute}.txt", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)


def parse_file_name(file_name: str):
    """ Extracts date and time from file name by parsing it"""
    minutes = file_name.split('.')[-3]
    hours = file_name.split('.')[-4]
    day = file_name.split('.')[-5] 
    month = file_name.split('.')[-6]
    year = file_name.split('.')[-7]
    return Timestamp(minutes=minutes, hours=hours, day=day, month=month, year=year)

def make_dataframe_from_blob(full_blob_path: str) -> pd.DataFrame:
    """ Creates a dataframe with selected info from blob"""
    full_df = pd.DataFrame(columns=['deviceId', 'enqueuedTime', 'telemetry'])
    with open(full_blob_path, 'r') as f:
        data = f.readlines()
    for nr, el in enumerate(data):
        el = el.split('","')
        for e  in el:
            name = e.split('":')[0].strip('"')
            value = e.split('":')[1].strip('"')
            if name == 'deviceId':
                full_df.loc[nr, 'deviceId'] = value
            if name == 'enqueuedTime':
                full_df.loc[nr, 'enqueuedTime'] = value
            if name == 'telemetry':
                full_df.loc[nr, 'telemetry'] = e.split('}}')[0].split('y":{"')[1] + '}'
    return full_df

def append_files(filenames: List[str], master_folder:str,
                 start_min:str, end_min:str, action:str):
    """ Receives a list of full file paths and appends them to a single file 
    stored in merged folder"""
    current_path = Path(__file__).parent.resolve()
    newpath = f'{current_path}\\merged'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    os.chdir(newpath)
    with open(f'{newpath}\\{action}_{master_folder}.{start_min}_{end_min}.txt', 'w') as outfile:
        for fname in filenames:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)

def merge_blobs(month:str, day: str, master_folder:str, start_hour:int,
                end_hour:int, start_min:int, end_min:int):

    """ Merge blobs between a given date and time, saves data to a new file in merged folder"""
    # downloads all blobs from given date, that day
    download_blobs_by_day(month=month, day=day, master_folder=master_folder)
    current_path = Path(__file__).parent.resolve()
    os.chdir(f'{current_path}\\{month}\\{day}') # recurse in folder where downloaded 
    # select blobs by time from already downloaded blobs
    all_files = []
    selected_files = [] # all files matching given timeframe
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            all_files.append(os.path.join(root, name))
    for file in all_files:
        file_hour = int(file.split('.')[-4])
        file_minutes = int(file.split('.')[-3])
        if (file_hour <= end_hour) and (file_hour >= start_hour):
            if (file_minutes <= end_min) and (file_minutes >=start_min):
                selected_files.append(os.path.join(os.getcwd(), file))
    

    two_up = Path(__file__).resolve().parents[1]
    os.chdir(two_up)
    append_files(selected_files, master_folder, start_min, end_min, 'URCARE')
    return selected_files




def main():     
    
    # pd = make_dataframe_from_blob(r'D:\Uni_life\MASTER\Anul_2\DISERTATIE\Project\src\src\03\18\fb1231ec-5b6b-476b-9751-71ae87ed1766.29.2022.03.28.16.36.62voms3a7mixw.txt')
    merge_blobs('06', '01', '29', 19, 19, 10, 12)
    n = 0 

if __name__ == "__main__":
    main()
