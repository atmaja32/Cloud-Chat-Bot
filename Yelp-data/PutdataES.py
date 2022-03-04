# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 22:49:23 2022

@author: atmaj
"""

import json
import boto3
import datetime
import requests
import decimal
from time import sleep
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


# In[4]:

region = 'us-east-1'
service = 'es'
credential = boto3.Session(aws_access_key_id="AKIAV7QZ727JS4L5MJY6",
                          aws_secret_access_key="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", 
                          region_name="us-east-1").get_credentials()
auth = AWS4Auth(credential.access_key, credential.secret_key, region, service)


# In[5]:


esEndPoint = 'search-restaurants-j6dg5bskheswtyt7gkxy7iogcu.us-east-1.es.amazonaws.com/'

# taken from stack overflow
es = OpenSearch(
    hosts = [{'host': esEndPoint, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
es.info()
es.ping()


# In[6]:


restaurants = {}
def addItems(data, cuisine):
    for rec in data:
            dataToAdd = {}
            try:
                if rec["alias"] in restaurants:
                    continue;
                dataToAdd['cuisine'] = cuisine
                dataToAdd['Business ID'] = str(rec["id"])
                sleep(0.001)
                print(dataToAdd)
                es.index(index="restaurants", doc_type="Restaurants", body=dataToAdd)
            except Exception as e:
                print(e)
        


# In[7]:


cuisines = ['indian', 'italian', 'mexican', 'chinese', 'american']
headers = {'Authorization': 'Bearer V3bJYCUd049CFfIyw8OPX7-RIy3HfxF3tlqgiIHsrOv6MmzW7xoi8M-YdgeUgiGqywPGCacFwSXy3MYeuCtkdB82pOaW_CCl3u21AVSf_R_qbiYUeXg7n0o07B8ZYnYx'}
DEFAULT_LOCATION = 'New York'
for cuisine in cuisines:
    for i in range(0, 100, 50):
        params = {'location': DEFAULT_LOCATION, 'offset': i, 'limit': 50, 'term': cuisine + " restaurants"}
        response = requests.get("https://api.yelp.com/v3/businesses/search", headers = headers, params=params)
        js = response.json()
        addItems(js["businesses"], cuisine)
