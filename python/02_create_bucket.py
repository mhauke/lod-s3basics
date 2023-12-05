#!/usr/bin/env python3

################################################################################
#
# Title:        02_create_bucket.py
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

    ### Step 5 - Create bucket
    try:            
        resp = s3.create_bucket(Bucket=conf_vars['bucket'])
    except ClientError as e:
        print('Error: {}'.format(e))
        exit()

    print("\n\n---- Response ----")
    pp.pprint(resp)

    ##################
    ##  Notice: There is no error if the bucket already exists
    ##  The bucket name must be unique across all existing bucket names in StorageGRID

    # check if bucket already exists
    # if s3.Bucket(conf_vars['BUCKET']) in s3.buckets.all():
    #     print('Bucket {} already exists'.format(conf_vars['BUCKET']))


    ### Step 6 - Print the list of buckets
    print('\n\nBucket {} created successfully'.format(conf_vars['bucket']))    



if __name__ == "__main__":
    main()
