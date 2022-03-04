import requests
#import csv
from datetime import datetime
import decimal
#import simplejson as json
import boto3


def check_empty(input):
	if len(str(input)) == 0:
		return 'N/A'
	else:
		return input

client = boto3.resource(service_name='dynamodb',
                          aws_access_key_id="AKIA2XBRXQ7VMTUD2QNC",
                          aws_secret_access_key="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",
                          region_name="us-east-1",
                         )
table = client.Table('yelp-restaurants')


business_id = "NjsXxWfNJIkkkKoqfelr6g"
API_KEY = 'V3bJYCUd049CFfIyw8OPX7-RIy3HfxF3tlqgiIHsrOv6MmzW7xoi8M-YdgeUgiGqywPGCacFwSXy3MYeuCtkdB82pOaW_CCl3u21AVSf_R_qbiYUeXg7n0o07B8ZYnYx' 
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/' # + {id}
HEADERS = {'Authorization': 'bearer %s' % API_KEY}


cuisines = ['italian', 'chinese', 'mexican', 'american','indian']

places = ['New York', 'Boston']

for place in places:
        for cuisine in cuisines:
             for i in range(0, 500, 50):
                 PARAMETERS = {'location': place, 'offset': i, 'limit': 50, 'term': cuisine + " restaurants"}
                 response = requests.get(url = ENDPOINT, params =  PARAMETERS, headers=HEADERS)
                 business_data = response.json()['businesses']
                 restauraunt_data = []
                 for business in business_data:
                     now = datetime.now()
                     dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                     table.put_item(
                         Item = {
                             'Business ID':check_empty(business['id']),
                             'insertedAtTimestamp': check_empty(dt_string),
                             'Name':  check_empty(business['name']),
                             'Cuisine': check_empty(cuisine),
                             'Rating': check_empty(decimal.Decimal(business['rating'])),
                             'Number of Reviews' : check_empty(decimal.Decimal(business['review_count'])),
                             'Address': check_empty(business['location']['address1']),
                             'Zip Code': check_empty(business['location']['zip_code']),
                             'Latitude': check_empty(str(business['coordinates']['latitude'])),
                             'Longitude': check_empty(str(business['coordinates']['longitude']))
                             }
                         )
