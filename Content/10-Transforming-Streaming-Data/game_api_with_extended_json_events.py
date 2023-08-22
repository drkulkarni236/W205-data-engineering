#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/purchase_a_frog")
def purchase_a_frog():
    purchase_frog_event = {'event_type': 'purchase_frog'}
    log_to_kafka('events', purchase_frog_event)
    return "Frog Purchased!\n"

## Example of POST
@app.route("/join_a_guild", methods = ['GET','POST'])
def join_guild():
    """
    We provide a GET and a POST API request form. The user can either use the join_a_guild GET method or the POST method (where the user can add some JSON text. 
    Both of those will be send to Kafka through the log_to_kafka function and will return a message to the user.
    """
    if request.method == 'GET':
        join_guild_event = {'event_type': 'join_guild'}
        log_to_kafka('events', join_guild_event)
        return "Join a Guild!\n"
    else:
        if request.headers['Content-Type'] == 'application/json':
            join_guild_event = {'event_type': 'join_guild', 'attributes': json.dumps(request.json)}
            log_to_kafka('events', join_guild_event)
            return "Join a guild!" + json.dumps(request.json) + "\n"
