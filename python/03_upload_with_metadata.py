#!/usr/bin/env python3

################################################################################
#
# Title:        03_upload_with_metadata.py
# Author:       Marko Hauke
# Date:         2023-12-04
# Description:  List all objects in a bucket
#
# SDK:          AWS Python SDK - boto3	
#		
# URLs:         https://docs.netapp.com/us-en/storagegrid-117/
#               https://boto3.amazonaws.com/v1/documentation/api/latest/index.html			
#
################################################################################

import os
import json
import boto3
import requests
import pprint as pp
from botocore.exceptions import ClientError


### Prep - Suppress HTTPs Warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def main():

    ### Prep - Set location for config file
    conf_file = '../config.json'

    # Load config from environment variables
    if all(k in os.environ for k in ("ENDPOINT","AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY" )):
        env_vars = {
            "ENDPOINT": os.environ.get('ENDPOINT'),
            "ACCESS_KEY": os.environ.get('AWS_ACCESS_KEY_ID'),
            "SECRET_ACCESS_KEY": os.environ.get('AWS_SECRET_ACCESS_KEY')
        }
        print("Found environment variables")
    else: 
        print("Error getting environment variables.")
        print("Aborting...")
        exit(1)

    ### Load config file 
    try:
        path = os.path.dirname(os.path.realpath(__file__))
        file = os.path.join(path, conf_file)
        with open(file) as json_file:
            conf_vars = json.load(json_file)
            print("Found config in config file")
    except IOError as e:
        print("Error reading config file: {}".format(e))
        print("Existing")
        exit(1)


    ### Create a boto3 session by specifying access key & secret key
    ### when AWS CLI is installed the credentials from ~/.aws/credentials 
    ### could be be used with profile_name='default' when creating the session
    session = boto3.session.Session(aws_access_key_id=env_vars['ACCESS_KEY'], aws_secret_access_key=env_vars['SECRET_ACCESS_KEY'])


    ### Create a resource client from the session 
    s3 = session.resource(service_name='s3', endpoint_url=env_vars['ENDPOINT'], verify=False)

    ### Step 5 - Loop thru files in source directory and upload to S3 bucket
    for root, dirs, files in os.walk(conf_vars['files'], topdown=True):
        file_count = 0
        uploaded_objects = []
        for f in files:
            fname, fext = os.path.splitext(f)
            if fext != '.JSON':
                try:
                    with open(f'{root}/{fname}.JSON') as json_file:
                        obj_metadata = json.load(json_file)
                    # upload the file to bucket 
                    s3.Bucket(conf_vars['bucket']).upload_file(
                                Filename = os.path.join(root, f), 
                                Key = f,
                                ExtraArgs={'Metadata': obj_metadata}
                                )
                    myobject = s3.Object(conf_vars['bucket'], f)
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print('Upload of {} failed'.format(f))
                    else:
                        print('Error: {}'.format(e))
                        exit()
                else:
                    # create a list of uploaded objects
                    uploaded_objects.append({'key': myobject.key, 'modify_date': myobject.last_modified , 'size': myobject.content_length, 'type': myobject.metadata['type'] })
                    file_count += 1


    ### Step 6 - Print the list of uploaded files
    print(f'\nUploaded Files & URLs:')
    print('{:30}   {:25}   {:17}   {}'.format('Object Key:', 'Last Modified:', 'Object Size:', 'Type:'))
    print('-' * 100) 
    for o in uploaded_objects:
        print('{:30}   {}   {:>12}   {:>10}'.format(o['key'], o['modify_date'], o['size'], o['type']))
    print(f'\nNumber of uploaded files: {file_count}\n')



if __name__ == "__main__":
    main()
