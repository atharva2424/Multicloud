import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1') 
table_name = "visit"
table = dynamodb.Table(table_name)


def lambda_handler(event, context):
    """
    AWS Lambda function to handle search, insert, and delete requests for the "visit" DynamoDB table.
    Expects an 'action' key in the event:
    - "add": Add a visit record of a user
    - "search": Retrieve visits for a user
    - "delete": Delete all visit records of a user
    """
    
    try:
        action = event.get("action")
        
        if action == "add":
            user_id = event.get("user_id")
            timestamp = event.get("timestamp")
            if not user_id or not timestamp:
                return {"statusCode": 400, "body": json.dumps("Missing 'user_id' or 'timestamp'")}

            response = add_item(user_id, timestamp)
            return {"statusCode": 200, "body": json.dumps(response)}

        elif action == "search":
            user_id = event.get("user_id")
            if not user_id:
                return {"statusCode": 400, "body": json.dumps("Missing 'user_id'")}

            records = search_table(user_id)
            return {"statusCode": 200, "body": json.dumps(records)}

        elif action == "delete":
            user_id = event.get("user_id")
            if not user_id:
                return {"statusCode": 400, "body": json.dumps("Missing 'user_id'")}

            response = delete_item(user_id)
            return {"statusCode": 200, "body": json.dumps(response)}

        else:
            return {"statusCode": 400, "body": json.dumps("Invalid action")}
    
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(str(e))}


def add_item(user_id, timestamp):
    """
    To Add an item to the DynamoDB table.
    """
    item = {
        "user_id": user_id,
        "timestamp": timestamp
    }
    table.put_item(Item=item)
    return {"message": "Item added successfully", "item": item}


def search_table(user_id):
    """
    To search for all records of a given user.
    """
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(user_id)
    )
    return response.get('Items', [])


def delete_item(user_id):
    """
    To deletes all records of a user from the DynamoDB table.
    """
    response = table.scan(
        FilterExpression=Key('user_id').eq(user_id)
    )
    for item in response['Items']:
        table.delete_item(Key={'user_id': user_id, 'timestamp': item['timestamp']})
    return {"message": "Item deleted successfully"}