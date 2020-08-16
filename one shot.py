#!/usr/bin/env python
# coding: utf-8

# In[6]:


from datetime import datetime
import requests
import logging
import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

def create_bucket(bucket_name, region=None):
    # Create bucket in a specific region
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_metadata_date(socrata_domain,socrata_dataset_identifier):
    metadata_url = f"https://{socrata_domain}/api/views/metadata/v1/{socrata_dataset_identifier}"
    r = requests.get(metadata_url)
    dataUpdatedAt = r.json()['dataUpdatedAt']
    ts = datetime.strptime(dataUpdatedAt, "%Y-%m-%dT%H:%M:%S%z")
    return str(int(ts.strftime("%Y%m%d%H%M%S")))

def check_s3(socrata_domain,socrata_dataset_identifier,last_update):
    item_name = "socrata-" + socrata_dataset_identifier
    bucket_name = item_name + "-" + last_update

    most_recent_check = False

    s3 = boto3.resource('s3')
    ## check if the bucket already exists
    for bucket in s3.buckets.all():
         if bucket.name == bucket_name:
                most_recent_check = True
    ## if not, make it        
    if not most_recent_check:
        create_bucket(bucket_name, region='us-east-2')

def fill_s3(socrata_domain,socrata_dataset_identifier,last_update):
    item_name = "socrata-" + socrata_dataset_identifier
    bucket_name = item_name + "-" + last_update
    
    s3 = boto3.resource('s3')
    
    bucket = s3.Bucket(bucket_name)
    key = item_name + '.json'
    
    ## check the the key (json data) already exists, if not fill it
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        pass
    else:
        #data_url = f"https://{socrata_domain}/api/views/metadata/v1/{socrata_dataset_identifier}"
        data_url = f"https://{socrata_domain}/resource/{socrata_dataset_identifier}.json"
        r = requests.get(data_url)
        data = r.json()
        s3object = s3.Object(bucket_name, key)
        s3object.put(Body=(bytes(json.dumps(data).encode('UTF-8'))), ContentType='application/json')


# In[7]:


socrata_domain = "data.cms.gov"
socrata_dataset_identifier = "77gb-8z53"

last_update = get_metadata_date(socrata_domain,socrata_dataset_identifier)

check_s3(socrata_domain,socrata_dataset_identifier,last_update)

fill_s3(socrata_domain,socrata_dataset_identifier,last_update)


# In[ ]:




