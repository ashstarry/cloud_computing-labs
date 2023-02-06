import json
import boto3

def lambda_handler(event, context):
    # print(event)
    source = event['currentIntent']['slots']['Source']
    destination = event['currentIntent']['slots']['Destination']
    print(f'{source}->{destination}')
    
    # DynamoDB
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    table_city_distance = dynamodb.Table('city_distance')
    
    response = None
    try:
        data = table_city_distance.get_item(
            Key={
                'source': source,
                'destination': destination
            }
        )
        
        dist = data['distance']

        response = {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
              "contentType": "SSML",
              "content": dist
            },
        }
    }
    except Exception as e:
        print(f'Error: {e}')
        response = {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
              "contentType": "SSML",
              "content": "Something is wrong"
            },
        }
    }
    
    print(f'response: {response}')
    return response