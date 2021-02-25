# Function to convert a CSV to JSON
import csv
import json
from google.cloud import pubsub_v1
from io import StringIO
from google.cloud import storage
import base64
import os
import requests


def make_json(request):
    data = {}
    count = 0
    result = []
    try:
        entity = request.args.get('entity')
        bucket_name = 'retail_sales_0102'
        file_name = entity + ' data-set.csv'
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob = blob.download_as_string()
        blob = blob.decode('utf-8')
        blob = StringIO(blob)
        csvReader = csv.DictReader(blob)
        print(f'Creating Json for topic: {entity}')
        # Convert each row into a dictionary
        # and publish it
        for rows in csvReader:
            data["topic"] = entity
            data["message"] = rows
            pub_input = json.dumps(data)
            print(f'Input to Publish(): {pub_input}')
            print('Calling Publish..')

            count += 1
            param = pub_input
            url = "https://us-central1-herewego-303119.cloudfunctions.net/publish"
            newHeaders = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            headers = newHeaders
            r = requests.post(url, data=param, headers=newHeaders)

    except Exception as e:
        print(e)
        return e, 500

    return f'Processed {count} messages for topic {entity}'


# Publishes a message to a Cloud Pub/Sub topic.
def publish(request):

    request_data = request.get_json()
    print(f'Request_data_json: {request_data}')
    PROJECT_ID = os.environ.get('PROJECT_ID')
    print(f'ProjectId {PROJECT_ID}')
    # Instantiates a Pub/Sub client
    publisher = pubsub_v1.PublisherClient()

    topic_name = request_data["topic"]
    message = request_data["message"]

    print(f'Request_data_topic: {topic_name}')
    print(f'Request_data_message: {message}')

    if not topic_name or not message:
        return ('Missing "topic" and/or "message" parameter.', 400)

    print(f'Publishing message to topic {topic_name}')

    # References an existing topic
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    print(f'Topic path {topic_path}')

    message_json = json.dumps(message)
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return e, 500


# def subscribe(event, context):
#     # Print out the data from Pub/Sub, to prove that it worked
#     print(base64.b64decode(event['data']))
# test commit






