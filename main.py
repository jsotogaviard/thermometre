import asyncio
import csv
from datetime import datetime
import boto3
import bleak
import botocore
import os
import csv
import json

s3 = boto3.client('s3')

in_dir = "./in"
out_dir = "./out"

BUCKET_NAME = "sotochassaignetest"  #TODO configurable time


class Thermometer:

    def __init__(self):
        self.time = 0
        self.key = 0
        self.mac = 0
        self.temperature = 0
        self.humidity = 0
        self.battery_percentage = 0
        self.battery_millivolts = 0
        self.counter = 0
        self.now = 0

    def toArray(self):
        return [
            self.time,
            self.key,
            self.mac,
            self.temperature,
            self.humidity,
            self.battery_percentage,
            self.battery_millivolts,
            self.counter
        ]

    def toHeaderArray(self):
        return [
            "time",
            "key",
            "mac",
            "temperature",
            "humidity",
            "battery_percentage",
            "battery_millivolts",
            "counter"
        ]


def filter_thermometer_devices(devices):
    thermometers = {}
    for device in devices:
        if device.metadata is not None and len(device.metadata['service_data']) != 0 and \
                list(device.metadata['service_data'].keys())[0] == '0000181a-0000-1000-8000-00805f9b34fb':
            thermometers[device.address] = list(device.metadata['service_data'].values())[0]
    return thermometers


def parse_data(now, key, value):
    thermometer = Thermometer()
    thermometer.key = key
    thermometer.now = now
    manufacturer_service_data_hex = value.hex()
    mac = manufacturer_service_data_hex[0:12]
    thermometer.mac = mac
    temperature = int(manufacturer_service_data_hex[12:12 + 4], 16) / 10
    thermometer.temperature = temperature
    humidity = int(manufacturer_service_data_hex[12 + 4:12 + 4 + 2], 16)
    thermometer.humidity = humidity
    battery_percentage = int(manufacturer_service_data_hex[12 + 4 + 2:12 + 4 + 2 + 2], 16)
    thermometer.battery_percentage = battery_percentage
    battery_millivolts = int(manufacturer_service_data_hex[12 + 4 + 2 + 2:12 + 4 + 2 + 2 + 4], 16)
    thermometer.battery_millivolts = battery_millivolts
    counter = int(manufacturer_service_data_hex[12 + 4 + 2 + 2 + 4:12 + 4 + 2 + 2 + 4 + 2], 16)
    thermometer.counter = counter
    time = now.strftime("%Y%m%d%H%M%S")
    thermometer.time = time
    print(thermometer.mac)
    return thermometer


def get_in_local_file_name(now):
    day = now.strftime("%Y%m%d")
    return './tmp/' + in_dir + '/' + day + '.csv'


def get_in_remote_file_name(now):
    day = now.strftime("%Y%m%d")
    return '/' + in_dir + '/' + day + '.csv'


# 1.0 function
def clean_local_directory(dir):
    in_files = [f for f in os.listdir(dir)]
    for in_file in in_files:
        os.remove(os.path.join(dir, in_file))

#1.2 function
async def scan_devices():
    scanner = bleak.BleakScanner()
    await scanner.start()
    await asyncio.sleep(10.0) #TODO configurable time
    await scanner.stop()
    return await scanner.get_discovered_devices()

#1.3 function
def download_from_in_or_create_in_file_locally(now):

    in_local_file_name = get_in_local_file_name(now)
    in_remote_file_name = get_in_remote_file_name(now)
    try:
        # download file from remote in directory
        s3.download_file(BUCKET_NAME, in_remote_file_name, in_local_file_name)
    except botocore.exceptions.ClientError as e:
        # create new empty file with headers
        thermometer = Thermometer()
        in_local_file = open(in_local_file_name, 'a+')
        with in_local_file:
            writer = csv.writer(in_local_file, lineterminator='\n')
            writer.writerows([thermometer.toHeaderArray()])
        in_local_file.close()


#1.4 function
def write_data_and_upload(now, thermometers):
    local_file_name = get_in_local_file_name(now)
    remote_file_name = get_in_remote_file_name(now)

    # open local file
    local_file = open(local_file_name, 'a+')
    with local_file:
        writer = csv.writer(local_file, lineterminator='\n')

        # write thermometer data
        for key, value in thermometers.items():
            thermometer = parse_data(now, key, value)
            writer.writerows([thermometer.toArray()])
    local_file.close()

    # upload file
    s3.upload_file(local_file_name, BUCKET_NAME, remote_file_name)

async def ble_to_in():

    # 1.0. Clean local in directory
    clean_local_directory(in_dir)

    # 1.1. Scan devices
    devices = await scan_devices()

    # 1.2. Filter thermometer devices
    thermometers = filter_thermometer_devices(devices)

    # 1.3. Download file locally from server if exists, otherwise create it with headers
    # The in structure contains one file per day
    # This file contains a thermometer data for each line
    now = datetime.now()
    download_from_in_or_create_in_file_locally(now)
    
    # 1.4. Write each thermometer in a new line and upload it
    write_data_and_upload(now, thermometers)


# 2.2 function
def download_in_files():
    in_files = s3.list_objects(Bucket=BUCKET_NAME,Prefix=in_dir)['Contents']
    for in_file in in_files:
        s3.download_file(BUCKET_NAME, in_file['Key'], in_dir + "/" + in_file['Key'])


# 2.3 function
def create_out_files():
    for root,dirs,in_files in os.walk(in_dir):
        
        for in_file_name in in_files:
            in_file = open(in_file_name, 'r')
            reader = csv.DictReader(in_file)
            for row in reader:
                json_data = json.dumps(row)

# 2.4 function
def upload_out_files():
    for root,dirs,out_files in os.walk(out_dir):
        for out_file in out_files:
            s3.upload_file(os.path.join(root,out_file), BUCKET_NAME, out_dir + "/" + out_file)


def in_to_out():

    # 2.1. Clean in and out files
    clean_local_directory(in_dir)
    clean_local_directory(out_dir)

    # 2.2 download in files
    download_in_files()

    # 2.3 create out files
    create_out_files()

    # 2.4 upload out files
    upload_out_files()


def run():

    # Retrieve bluetooth advertising data
    # and put in aws s3 in folder
    ble_to_in()

    # Transform in data format 
    # to out data format
    in_to_out()
    

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
