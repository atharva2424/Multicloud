import datetime

from flask import Flask, render_template

from google.cloud import datastore

'''This code is written to access the google cloud datastore and perform operations on it'''

datastore_client = datastore.Client()

app = Flask(__name__)


def fetch_visits(limit):
    query = datastore_client.query(kind="visit")
    query.order = ["-timestamp"]

    visits = list(query.fetch(limit=limit))

    return visits

def store_visit(user_id, dt):
    entity = datastore.Entity(key=datastore_client.key("visit"))
    entity.update({"user_id": user_id, "timestamp": dt})

    datastore_client.put(entity)

def delete_user_visits(user_id):
    query = datastore_client.query(kind="visit")
    query.add_filter("user_id", "=", user_id)
    visits = query.fetch()

    for visit in visits:
        datastore_client.delete(visit.key)

def search_user_visits(user_id):
    query = datastore_client.query(kind="visit")
    query.add_filter("user_id", "=", user_id) 
    query.order = ["-timestamp"]
    visits = list(query.fetch())
    visits_list = [{"user_id": visit["user_id"], "timestamp": visit["timestamp"]} for visit in visits]
    return visits_list

@app.route("/visit/<user_id>")
def root(user_id):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    store_visit(user_id, timestamp)

    visits = fetch_visits(10)

    return render_template("index.html", visits=visits)

@app.route("/delete/<user_id>")
def delete(user_id):
    delete_user_visits(user_id)
    return f"All visits for user {user_id} have been deleted."

@app.route("/search/<user_id>")
def search(user_id):
    visits = search_user_visits(user_id)
    return f"visits for the {user_id} {visits}"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)