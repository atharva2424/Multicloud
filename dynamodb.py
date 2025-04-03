import datetime
import requests

from flask import Flask

'''This code was written to access and perform operations on the Amazon DynamoDB'''

app = Flask(__name__)


AWS_LAMBDA_API_URL = "https://yo4fu6xdvc.execute-api.us-east-1.amazonaws.com/dev/DynamoDBHandler"

def store_in_dynamodb(user_id, timestamp):
    payload = {
        "action": "add",
        "user_id": user_id,
        "timestamp": timestamp
    }
    response = requests.post(AWS_LAMBDA_API_URL, json=payload)
    return response.json()

def delete_from_dynamodb(user_id):
    payload = {
        "action": "delete",
        "user_id": user_id
    }
    response = requests.post(AWS_LAMBDA_API_URL, json=payload)
    return response.json()

def search_in_dynamo(user_id):
    payload = {
        "action": "search",
        "user_id": user_id
    }
    response = requests.post(AWS_LAMBDA_API_URL, json=payload)
    return response.json()


@app.route("/visit/<user_id>")
def root(user_id):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    store_in_dynamodb(user_id, timestamp)
    return f"added {user_id}"

@app.route("/delete/<user_id>")
def delete(user_id):
    delete_from_dynamodb(user_id)
    return f"All visits for user {user_id} have been deleted."

@app.route("/search/<user_id>")
def search(user_id):
    visits = search_in_dynamo(user_id)
    return f"visits for the {user_id} {visits}"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)