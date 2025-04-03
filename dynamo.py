from boto3 import resource
from boto3.dynamodb.conditions import Key

'''This code was used to create a access a dynamo table initially'''

def create_table(table_name):
    ''' Create a table and return the table object '''
    dynamodb_resource = resource('dynamodb', region_name='us-east-1')

    existing_tables = [table.name for table in dynamodb_resource.tables.all()]
    if table_name in existing_tables:
        print(f"Table '{table_name}' already exists.")
        return dynamodb_resource.Table(table_name)

    table = dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'user_id', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}  # Sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'user_id', 'AttributeType': 'S'},  # String
            {'AttributeName': 'timestamp', 'AttributeType': 'S'}  # String
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Waiting for table creation...")
    table.wait_until_exists()
    print(f"Table '{table_name}' created successfully!")

    return table


def get_table(table_name):
    ''' Return the table object if exists, otherwise create and return it '''
    dynamodb_resource = resource('dynamodb', region_name='us-east-1')  

    try:
        table = dynamodb_resource.Table(table_name)
        table.load()  
        print(f"Table '{table_name}' found.")
    except:
        print(f"Table '{table_name}' not found. Creating new table...")
        table = create_table(table_name)

    return table


def search_table(table, user_id):
    ''' Search for records associated with a given user_id '''
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(user_id)
    )
    return response.get('Items', [])


def add_item(table, user_id, timestamp):
    ''' Add one item (row) to the table '''
    item = {
        'user_id': user_id,
        'timestamp': timestamp
    }
    response = table.put_item(Item=item)
    return response


def delete_item(table, user_id, timestamp):
    ''' Delete an item from the table using its primary key '''
    response = table.delete_item(
        Key={
            'user_id': user_id,
            'timestamp': timestamp
        }
    )
    return response

def main():
    table_name = "visit"
    
    table = get_table(table_name)

    user_id = "user123"
    timestamp = "2025-04-02T12:00:00Z"
    print("Adding an item...")
    add_item(table, user_id, timestamp)

    print(f"Searching records for user_id '{user_id}'...")
    records = search_table(table, user_id)
    print("Records found:", records)

    print(f"Deleting record with user_id '{user_id}' and timestamp '{timestamp}'...")
    delete_item(table, user_id, timestamp)

    print(f"Searching records for user_id '{user_id}' after deletion...")
    records = search_table(table, user_id)
    print("Records found:", records)


if __name__ == "__main__":
    main()