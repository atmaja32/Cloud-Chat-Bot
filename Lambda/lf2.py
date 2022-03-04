import json
import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.vendored import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getSQS():
    SQS = boto3.client("sqs")
    url = 'https://sqs.us-east-1.amazonaws.com/411297634259/deeptisqs'
    response = SQS.receive_message(
        QueueUrl=url,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    SQS.delete_message(
            QueueUrl=url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('Received and deleted message: %s' % response)
    logger.debug("message: {}".format(message))
    return message

def lambda_handler(event, context):

    """
        Query SQS to get the messages
        Store the relevant info, and pass it to the Elastic Search
    """

    message = getSQS()
    if message is None:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return

    message_body = json.loads(message["Body"])
    logger.debug("cuisine: {}".format(message_body["cuisine"]))
    cuisine = message_body["cuisine"]
    print(cuisine)

    logger.debug(cuisine)

    location = message_body["location"]
    time = message_body["time"]
    party = message_body["party"]
    phone = message_body["phone"]
    if not cuisine or not phone:
        logger.debug("No Cuisine or Phone found in message")
        return
    print(location)

    es = 'https://search-restaurants-j6dg5bskheswtyt7gkxy7iogcu.us-east-1.es.amazonaws.com/_search?q={cuisine}'.format(cuisine = cuisine)
    esResponse = requests.get(es, auth=("deept","Deepti3132$"))
    logger.debug("esResponse: {}".format(esResponse.text))
    data = json.loads(esResponse.content.decode('utf-8'))
    logger.info("data: {}".format(data))
    try:
        esData = data["hits"]["hits"]
        logger.info("esData: {}".format(esData))
    except KeyError:
        logger.debug("Error extracting hits from ES response")

    resids = []
    for restaurant in esData:
        resids.append(restaurant["_source"]["Business ID"])

    messageToSend = 'Hello! Here are the {cuisine} restaurant suggestions in {location} for {numPeople} people, at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=party,
            diningTime=time,
        )

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    itr = 1
    for id in resids:
        if itr == 6:
            break
        response = table.scan(FilterExpression=Attr('Buisness ID').eq(id))
        logger.info("dynamodb response: {}".format(response))
        item = response['Items'][0]
        if response is None:
            continue
        restaurantMsg = '' + str(itr) + '. '
        name = item["Name"]
        address = item["Address"]
        restaurantMsg += name +', located at ' + address +'. '
        messageToSend += restaurantMsg
        itr += 1

    messageToSend += "Have a nice meal!!"
    logger.info("messageToSend: {}".format(messageToSend))

    try:
        client = boto3.client('sns')
        response = client.publish(
            PhoneNumber=phone,
            Message= messageToSend,
            MessageStructure='string'
        )
    except KeyError:
        logger.debug("Error sending ")
    logger.debug("response - %s",json.dumps(response) )
    logger.debug("Message = '%s' Phone Number = %s" % (messageToSend, phone))

    return {
        'statusCode': 200,
        'body': json.dumps(messageToSend)
    }
