from main import *
import os
import boto3

s3 = boto3.resource('s3')


def test_download_or_create_file_locally_no_remote_file_no_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # Prepare environment
    # remove local file if necessary
    local_file_name = create_local_file_name(thermo)
    try:
        os.remove(local_file_name)
    except FileNotFoundError as e:
        print(e)

    # remove remote file if necessary
    remote_file_name = create_remote_file_name(thermo)
    try:
        s3.Object(BUCKET_NAME, remote_file_name).delete()
    except FileNotFoundError as e:
        print(e)

    # execute the function
    download_or_create_file_locally(thermo)

    # make sure file exists and it is empty
    assert os.path.isfile(local_file_name)

    # make sure file is empty
    assert os.stat(local_file_name).st_size == 0


def test_download_or_create_file_locally_no_remote_file_yes_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # Prepare
    # create local file
    local_file_name = create_local_file_name(thermo)
    file = open(local_file_name, "w")
    file.write("a,b,c,d,e,f")
    file.close()

    # remove remote file
    remote_file_name = create_remote_file_name(thermo)
    try:
        s3.Object(BUCKET_NAME, remote_file_name).delete()
    except FileNotFoundError as e:
        print(e)

    # execute the function
    download_or_create_file_locally(thermo)

    # make sure file exists
    assert os.path.isfile(local_file_name)

    # make sure file is empty
    assert os.stat(local_file_name).st_size == 0


def test_download_or_create_file_locally_yes_remote_file_yes_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # create local file
    local_file_name = create_local_file_name(thermo)
    file = open(local_file_name, "a")
    file.write("a,b,c,d,e,f")
    file.close()

    # create remote file
    remote_file_name = create_remote_file_name(thermo)
    s3_client.upload_file(local_file_name, BUCKET_NAME, remote_file_name)

    # write other data in local file
    file = open(local_file_name, "a")
    file.write("totot")
    file.close()

    # localfile contains totot
    # remote file contains a,b,c,d,e,f
    # execute the function
    download_or_create_file_locally(thermo)

    # local file must exist and its content must be equal to a,b,c,d,e,f
    # make sure file exists
    assert os.path.isfile(local_file_name)

    # make sure file is empty
    with open(local_file_name) as myfile:
        if 'a,b,c,d,e,f' in myfile.read():
            assert True
        else:
            assert False


def test_download_or_create_file_locally_yes_remote_file_no_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # create local file
    local_file_name = create_local_file_name(thermo)
    file = open(local_file_name, "a")
    file.write("a,b,c,d,e,f")
    file.close()

    # create remote file
    remote_file_name = create_remote_file_name(thermo)
    s3_client.upload_file(local_file_name, BUCKET_NAME, remote_file_name)

    # write other data in local file
    try:
        os.remove(local_file_name)
    except FileNotFoundError as e:
        print(e)

    # there is no localfile
    # remote file contains a,b,c,d,e,f
    # execute the function
    download_or_create_file_locally(thermo)

    # local file must exist and its content must be equal to a,b,c,d,e,f
    # make sure file exists
    assert os.path.isfile(local_file_name)

    # make sure file is empty
    with open(local_file_name) as myfile:
        if 'a,b,c,d,e,f' in myfile.read():
            assert True
        else:
            assert False


def test_write_data_and_upload_empty_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # remove local file
    local_file_name = create_local_file_name(thermo)
    try:
        os.remove(local_file_name)
    except FileNotFoundError as e:
        print(e)

    # create empty local file
    file = open(local_file_name, "a")
    file.close()

    write_data_and_upload(thermo)

    remote_file_name = create_remote_file_name(thermo)
    to_check = './tmp/to_check.txt'
    s3_client.download_file(BUCKET_NAME, remote_file_name, to_check)
    with open(to_check) as myfile:
        content = myfile.read()
        if '0,0,mac,0,0,0,0,0' in content:
            assert True
        else:
            print(content)
            assert False


def test_write_data_and_upload_not_empty_local_file():
    thermo = Thermometer()
    thermo.mac = "mac"
    thermo.now = date.today()

    # remove local file
    local_file_name = create_local_file_name(thermo)
    try:
        os.remove(local_file_name)
    except FileNotFoundError as e:
        print(e)

    # create empty local file
    file = open(local_file_name, "a")
    file.write("a,b,c,d,e\n")
    file.close()

    write_data_and_upload(thermo)

    remote_file_name = create_remote_file_name(thermo)
    to_check = './tmp/to_check.txt'
    s3_client.download_file(BUCKET_NAME, remote_file_name, to_check)
    with open(to_check) as myfile:
        content = myfile.read()
        if 'a,b,c,d,e\n0,0,mac,0,0,0,0,0' in content:
            assert True
        else:
            print(content)
            assert False
