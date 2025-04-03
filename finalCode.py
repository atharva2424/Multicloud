import datetime
import requests

from flask import Flask, render_template

from google.cloud import datastore

'''This code was written to access google datastore and amazon DynamoDb and perform operations on them simultaneously and is deployed on the app engine'''

datastore_client = datastore.Client()

app = Flask(__name__)


AWS_LAMBDA_API_URL = "https://yo4fu6xdvc.execute-api.us-east-1.amazonaws.com/dev/DynamoDBHandler"

def store_visit(user_id, dt):
    entity = datastore.Entity(key=datastore_client.key("visit"))
    entity.update({"user_id": user_id, "timestamp": dt})

    datastore_client.put(entity)

def store_in_dynamodb(user_id, timestamp):
    payload = {
        "action": "add",
        "user_id": user_id,
        "timestamp": timestamp
    }
    response = requests.post(AWS_LAMBDA_API_URL, json=payload)
    return response.json()

def fetch_visits(limit):
    query = datastore_client.query(kind="visit")
    query.order = ["-timestamp"]

    visits = list(query.fetch(limit=limit))

    return visits

def delete_user_visits(user_id):
    query = datastore_client.query(kind="visit")
    query.add_filter("user_id", "=", user_id)
    visits = query.fetch()

    for visit in visits:
        datastore_client.delete(visit.key)

def delete_from_dynamodb(user_id):
    payload = {
        "action": "delete",
        "user_id": user_id
    }
    response = requests.post(AWS_LAMBDA_API_URL, json=payload)
    return response.json()

def search_user_visits(user_id):
    query = datastore_client.query(kind="visit")
    query.add_filter("user_id", "=", user_id) 
    query.order = ["-timestamp"]
    visits = list(query.fetch())
    visits_list = [{"user_id": visit["user_id"], "timestamp": visit["timestamp"]} for visit in visits]
    return visits_list

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
    store_visit(user_id, timestamp)
    store_in_dynamodb(user_id, timestamp)

    visits = fetch_visits(10)

    return render_template("index.html", visits=visits)

@app.route("/delete/<user_id>")
def delete(user_id):
    delete_user_visits(user_id)
    delete_from_dynamodb(user_id)
    return f"All visits for user {user_id} have been deleted."

@app.route("/search/<user_id>")
def search(user_id):
    visits = search_user_visits(user_id)
    visits1 = search_in_dynamo(user_id)
    return f"Visits for {user_id} from datastore:<br>{visits}<br><br>Visits from DynamoDB:<br>{visits1['body']}"






if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)