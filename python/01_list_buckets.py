#!/usr/bin/env python3

################################################################################
#
# Title:        01_list_buckets.py
# Author:       Marko Hauke
# Date:         2023-12-04
# Description:  List all Buckets for tenant
#
# Modules:      json, os, boto3	
#		
# URLs:         https://docs.netapp.com/us-en/storagegrid-117/
#               https://boto3.amazonaws.com/v1/documentation/api/latest/index.html			
#
################################################################################

import os
import boto3
import requests
from botocore.exceptions import ClientError


### Prep - Suppress HTTPs Warnings for self-signed certificates
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def main():

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
 
    ### Create a boto3 session by specifying access key & secret key
    ### when AWS CLI is installed the credentials from ~/.aws/credentials 
    ### could be be used with profile_name='default' when creating the session
    session = boto3.session.Session(aws_access_key_id=env_vars['ACCESS_KEY'], aws_secret_access_key=env_vars['SECRET_ACCESS_KEY'])


    ### Create a resource client from the session 
    s3 = session.resource(service_name='s3', endpoint_url=env_vars['ENDPOINT'], verify=False)


    ### Get a list of all buckets
    bucket_iter = s3.buckets.all()

    bucket_list = {}
    try:
        for bucket in bucket_iter:
            bucket_list[bucket.name] = {'create_date': bucket.creation_date}
            # print(bucket.Versioning().load())

    except ClientError as e:
        print('Error: {}'.format(e))
        exit()


    ### Sort buckets by create time
    sorted_buckets = {k: v for k, v in sorted(bucket_list.items(), key=lambda item: item[1]['create_date'])}


    ### Print the list of buckets
    print('\nExisting Buckets:')
    print('{:30}   {}'.format('Bucket Name:', 'Create Date:'))
    print('-' * 65) 
    for k,v in sorted_buckets.items():
        print('{:30}   {}'.format(k, v['create_date']))



if __name__ == "__main__":
    main()

