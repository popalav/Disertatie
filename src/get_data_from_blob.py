import os
from collections import namedtuple
from typing import List
from pathlib import Path
import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient
from utilities import Actions
import re
import glob 


N_STR = 'BlobEndpoint=https://disertatiestorageaccount.blob.core.windows.net/;QueueEndpoint=https://disertatiestorageaccount.queue.core.windows.net/;FileEndpoint=https://disertatiestorageaccount.file.core.windows.net/;TableEndpoint=https://disertatiestorageaccount.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-03-18T07:37:21Z&st=2022-03-17T23:37:21Z&spr=https&sig=uvHB8JM97JDU6%2BPSVGQheKH5PtAuTlyStJYyjgh%2B9I4%3D'
BLOB_CONN_STR = 'BlobEndpoint=https://disertatiestorageaccount.blob.core.windows.net/;QueueEndpoint=https://disertatiestorageaccount.queue.core.windows.net/;FileEndpoint=https://disertatiestorageaccount.file.core.windows.net/;TableEndpoint=https://disertatiestorageaccount.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-10-08T13:59:02Z&st=2022-04-13T05:59:02Z&spr=https,http&sig=oIjqHMLHKmZRVNhbSze4PzBtTo6F%2BZHZ01plxAucRR4%3D'
CONTAINER = ContainerClient.from_connection_string(conn_str=BLOB_CONN_STR, container_name="disertatie")

Split = namedtuple("Split", "minutes hours day month year masterfolder ")


blob = BlobClient.from_connection_string(conn_str=BLOB_CONN_STR,
                                         container_name="disertatie",
                                         blob_name="fb1231ec-5b6b-476b-9751-71ae87ed1766/14/2022/03/18/00/22/wyta6lqqsiw6i")
def get_all_blobs():
    """ Returns an iterator with all blobs from a container """
    blob_list = CONTAINER.list_blobs()
    return blob_list

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
    newpath = f'{current_path}\\{master_folder}\\{month}\\{day}'
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
    minutes = file_name.split('/')[-2]
    hours = file_name.split('/')[-3]
    day = file_name.split('/')[-4] 
    month = file_name.split('/')[-5]
    year = file_name.split('/')[-6]
    masterfolder = file_name.split('/')[-7]
    return Split(minutes=minutes, hours=hours, day=day, month=month, year=year, masterfolder=masterfolder)

def make_dataframe_from_blob(full_blob_path: str, action:str) -> pd.DataFrame:
    """ Creates a dataframe with selected info from blob"""
    full_df = pd.DataFrame(columns=['deviceId', 'action', 'enqueuedTime', 
                                    'acc_x', 'acc_y', 'acc_z'])
                                    # , 'gyr_x', 
                                    # 'gyr_y', 'gyr_z', 'mag_x', 'mag_y', 
                                    # 'mag_z', 'geo_alt', 'geo_lat', 'geo_lon'])
    with open(full_blob_path, 'r') as f:
        data = f.readlines()
    for nr, el in enumerate(data):
        el = el.split('","')
        row_dictionary = {}
        for e  in el:
            name = e.split('":')[0].strip('"')
            value = e.split('":')[1].strip('"')
            # dict to hold one row of df worth of info
            if name == 'deviceId':
                # full_df.loc[nr, 'deviceId'] = value
                row_dictionary['deviceId'] = value
                row_dictionary['action'] = action
            if name == 'enqueuedTime':
                # full_df.loc[nr, 'enqueuedTime'] = value
                row_dictionary['enqueuedTime'] = value
            if name == 'telemetry':
                telemetry = e.split('}}')[0].split('y":{"')[1] + '}'
                battery_case = re.search('battery', value)
                geolocation_case = re.search('geolocation',telemetry)
                barometer_case = re.search('barometer',telemetry)
                if battery_case is None and geolocation_case is None and barometer_case is None:
                    sensor_type = telemetry.split('":{"')[0]
                    sensor_values = telemetry.split('":{"')[1]
                    # daca tot randul de telemetry contine battery sau geolocation  nu se adauga randul deloc in da
                    # taframe, daca nu se merge si se adauga tot ce e necesar       
                    if sensor_type == 'accelerometer':
                        # row_dictionary['telemetry'] = telemetry
                        row_dictionary['acc_x'] = sensor_values.split(',')[0].split(':')[1]
                        row_dictionary['acc_y'] = sensor_values.split(',')[1].split(':')[1]
                        row_dictionary['acc_z'] = sensor_values.split(',')[2].split(':')[1].strip('}')
                    """ 
                    elif sensor_type == 'gyroscope':
                        row_dictionary['gyr_x'] = sensor_values.split(',')[0].split(':')[1]
                        row_dictionary['gyr_y'] = sensor_values.split(',')[1].split(':')[1]
                        row_dictionary['gyr_z'] = sensor_values.split(',')[2].split(':')[1]
                    elif sensor_type == 'magnetometer':
                        row_dictionary['mag_x'] = sensor_values.split(',')[0].split(':')[1]
                        row_dictionary['mag_y'] = sensor_values.split(',')[1].split(':')[1]
                        row_dictionary['mag_z'] = sensor_values.split(',')[2].split(':')[1]
                    """
        # append te dict to the df only if the telemetry column is present
        if 'acc_x' in row_dictionary.keys():
            full_df = full_df.append(row_dictionary, ignore_index=True)
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
                end_hour:int, start_min:int, end_min:int, action: str):

    """ Merge blobs between a given date and time, saves data to a new file in merged folder"""
    # downloads all blobs from given date, that day
    # download_blobs_by_day(month=month, day=day, master_folder=master_folder)
    current_path = Path(__file__).parent.resolve()
    os.chdir(f'{current_path}\\all_blobs\\{master_folder}\\{month}\\{day}') # recurse in folder where downloaded 
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
    append_files(selected_files, master_folder, start_min, end_min, action)
    return selected_files

def blob_to_csv(full_blob_path:str, action:str):
    """ Selects only needed needed info from merged blob usually
    and it transform to a df then it writes the df to a csv"""
    current_path = Path(__file__).parent.resolve()
    newpath = f'{current_path}\\csv'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    df = make_dataframe_from_blob(full_blob_path, action)
    os.chdir(f'{current_path}\\csv')
    csv_name = full_blob_path.split('\\')[-1]
    df.to_csv(f'{csv_name}.csv')

def download_all_blobs():
    """ Downloads all blobs and place them accordingly to master folder, month and day"""
    # download all blobs and created folder by master folder, month and day 
    current_path = Path(__file__).parent.resolve()
    newpath = f'{current_path}\\all_blobs'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    os.chdir(newpath)
    all_blobs = get_all_blobs()
    for blob in all_blobs:
            split = parse_file_name(blob.name)
            if split.masterfolder != '14':
                newpath = f'{current_path}\\all_blobs\\{split.masterfolder}'
                if not os.path.exists(newpath):
                    os.makedirs(newpath)
                os.chdir(newpath)
                newpath = f'{current_path}\\all_blobs\\{split.masterfolder}\\{split.month}'
                if not os.path.exists(newpath):
                    os.makedirs(newpath)
                os.chdir(newpath)
                newpath = f'{current_path}\\all_blobs\\{split.masterfolder}\\{split.month}\\{split.day}'
                if not os.path.exists(newpath):
                    os.makedirs(newpath)
                os.chdir(newpath)
                download_blob_by_name(blob.name)

def merge_label_data():
    """ Merge blobs by category and creates a new blob labeled """
    ########################## STEPPER ##########################
    # Edi Stepper 1
    merge_blobs(month='06', day='01', master_folder='18', start_hour=16,
                end_hour=16, start_min=3, end_min=5, action= 'STEPPER')
    # Edi Stepper 2
    merge_blobs(month='06', day='01', master_folder='18', start_hour=16,
                end_hour=16, start_min=17, end_min=20, action= 'STEPPER')
    # Paul Stepper 1
    merge_blobs(month='06', day='15', master_folder='19', start_hour=17,
                end_hour=17, start_min=50, end_min=55, action= 'STEPPER')
    # Paul Stepper 2
    merge_blobs(month='06', day='15', master_folder='19', start_hour=18,
                end_hour=18, start_min=7, end_min=11, action= 'STEPPER')
    # Mara Stepper
    merge_blobs(month='06', day='15', master_folder='4', start_hour=17,
                end_hour=17, start_min=31, end_min=41, action= 'STEPPER')
    # Darius Stepper 1
    merge_blobs(month='06', day='15', master_folder='5', start_hour=17,
                end_hour=17, start_min=22, end_min=28, action= 'STEPPER')
    # Darius Stepper 2
    merge_blobs(month='06', day='15', master_folder='5', start_hour=17,
                end_hour=17, start_min=42, end_min=47, action= 'STEPPER')
    # Lavi Stepper 1
    merge_blobs(month='06', day='01', master_folder='29', start_hour=16,
                end_hour=16, start_min=6, end_min=16, action= 'STEPPER')
    # Lavi Stepper 2
    merge_blobs(month='06', day='01', master_folder='29', start_hour=16,
                end_hour=16, start_min=22, end_min=28, action= 'STEPPER')
    # Ana Stepper 1
    merge_blobs(month='08', day='30', master_folder='12', start_hour=12,
                end_hour=12, start_min=13, end_min=16, action= 'STEPPER')
    ########################## LAYING ##########################
    # Paul Laying
    merge_blobs(month='07', day='14', master_folder='19', start_hour=16,
                end_hour=16, start_min=24, end_min=35, action= 'LAYING')
    ########################## SITTING ##########################
    # Paul Sitting
    merge_blobs(month='07', day='14', master_folder='19', start_hour=16,
                end_hour=16, start_min=1, end_min=16, action= 'SITTING')
    # Mara Sitting
    merge_blobs(month='06', day='15', master_folder='4', start_hour=16,
                end_hour=16, start_min=3, end_min=16, action= 'SITTING')
    # Darius Sitting
    merge_blobs(month='08', day='03', master_folder='5', start_hour=17,
                end_hour=17, start_min=48, end_min=58, action= 'SITTING')
    ########################## WALKING ##########################
    # Paul Walking
    merge_blobs(month='06', day='15', master_folder='19', start_hour=16,
                end_hour=16, start_min=1, end_min=16, action= 'WALKING')
    # Mara Walking
    merge_blobs(month='06', day='15', master_folder='4', start_hour=16,
                end_hour=16, start_min=3, end_min=16, action= 'WALKING')
    # Darius Walking
    merge_blobs(month='06', day='15', master_folder='5', start_hour=17,
                end_hour=17, start_min=48, end_min=58, action= 'WALKING')
    ########################## STANDING ##########################
    # Darius Standing 
    merge_blobs(month='06', day='15', master_folder='5', start_hour=16,
                end_hour=17, start_min=57, end_min=9, action= 'STANDING')
    # Mara Standing
    merge_blobs(month='06', day='15', master_folder='4', start_hour=16,
                end_hour=16, start_min=37, end_min=47, action= 'STANDING')
    # Paul Walking
    merge_blobs(month='06', day='15', master_folder='19', start_hour=16,
                end_hour=16, start_min=37, end_min=47, action= 'STANDING')

def main():   
    # Call this only at first run
    # download_all_blobs()
    
    merge_label_data()

    current_path = Path(__file__).parent.resolve()
    os.chdir(f'{current_path}\\merged')
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            full_path = os.path.abspath(f'{current_path}\\merged\\{name}')
            blob_to_csv(full_blob_path=full_path, action=name.split('_')[0])

    files = os.path.join(f'{current_path}\\csv', "*.csv")
    # list of merged files returned
    files = glob.glob(files)

    print("Resultant CSV after joining all CSV files at a particular location...");

    # joining files with concat and read_csv
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    print(df)
    n = 0
if __name__ == "__main__":
    main()
