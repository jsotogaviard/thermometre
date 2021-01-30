import asyncio
import csv
from datetime import datetime
import boto3
import bleak
import botocore
import os

s3_client = boto3.client('s3')

in_dir = "./in"

BUCKET_NAME = "sotochassaignetest"


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
            self.time,
            self.key,
            self.mac,
            self.temperature,
            self.humidity,
            self.battery_percentage,
            self.battery_millivolts,
            self.counter
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
    time = now.strftime("%Y%d%m%H%M%S")
    thermometer.time = time
    print(thermometer.mac)
    return thermometer


def get_in_local_file_name(now):
    day = now.strftime("%Y%m%d")
    return './tmp/' + in_dir + '/' + day + '.csv'


def get_in_remote_file_name(now):
    day = now.strftime("%Y%m%d")
    return '/' + in_dir + '/' + day + '.csv'


def write_data_and_upload(now, thermometers_data):
    local_file_name = create_local_file_name(now)
    remote_file_name = create_remote_file_name(now)
    my_file = open(local_file_name, 'a+')
    with my_file:
        writer = csv.writer(my_file, lineterminator='\n')
        for thermometer in thermometers_data:
            writer.writerows([thermometer.toArray()])
    my_file.close()
    s3_client.upload_file(local_file_name, BUCKET_NAME, remote_file_name)


# 1.0 function
def clean_in_local_directory():
    in_files = [f for f in os.listdir(in_dir)]
    for in_file in in_files:
        os.remove(os.path.join(in_dir, in_file))

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
        s3_client.download_file(BUCKET_NAME, in_remote_file_name, in_local_file_name)
    except botocore.exceptions.ClientError as e:
        # create new empty file with headers
        thermometer = Thermometer()
        my_file = open(in_local_file_name, 'a+')
        with my_file:
            writer = csv.writer(my_file, lineterminator='\n')
            writer.writerows([thermometer.toArrayHeaders()])
        f = open(in_local_file_name, 'a+')
        f.close()


async def ble_to_in():
    # 0. Clean local in directory
    clean_in_local_directory()

    # 1. Scan devices
    devices = await scan_devices()

    # 2. Filter thermometer devices
    thermometers = filter_thermometer_devices(devices)

    # 3. Download file locally from server if exists, otherwise create it with headers
    # The in structure contains one file per day
    # This file contains a thermometer data for each line
    now = datetime.now()
    download_from_in_or_create_in_file_locally(now)
    
    # Main loop
    thermometers_data = []
    for key, value in thermometers.items():

        # parse manufacturer service data
        thermometer = parse_data(now, key, value)
        thermometers_data.push(thermometer)
    
    # write data and upload it
    write_data_and_upload(now, thermometers_data)


def download_in_files():
    list=s3_client.list_objects(Bucket=BUCKET_NAME,Prefix='out')['Contents']
    for key in list:
        s3_client.download_file(BUCKET_NAME, key['Key'], "./in/" + key['Key'])


def create_out_files():



def upload_out_files():
    

def in_to_out():

    # download in files
    download_in_files()

    # create out files
    create_out_files()

    # upload out files
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
