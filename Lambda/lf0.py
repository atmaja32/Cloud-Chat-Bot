# import the json utility package since we will be working with a JSON object
import json
# import two packages to help us with dates and date formatting
from time import gmtime, strftime

now = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())

# define the handler function that the Lambda service will use as an entry point
def lambda_handler(event, context):
  return {
        'statusCode': 200,
        'messages': [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "1",
                    "text": "I’m still under development.",
                    "timestamp": now
                }
            }
            ],
        'headers':{
            'Access-Control-Allow-Headers':'Content-Type',
            'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }
    }
