import unittest
import os
from src import consumer
import json
import jsonschema
import boto3

# created as a global for 3 reasons:
# 1. Every test function uses the exact same configuration
# 2. The logger prints multiple outputs when multiple handlers are active at runtime.
#    Having one logger for every function prevents this.
# 3. unittest doesn't allow extra arguments in the test functions
logger = consumer.create_logger(debug=True, save_file="./logs/test_consumer.log")


# loads and returns all valid sample requests as list
# if the specified request_type is given, only returns valid requests of that type
def get_test_sample_requests(request_type=None):
    request_dir = "sample-requests"
    request_files = os.listdir(request_dir)

    with open("./schemas/request-schema.json") as schema_file:
        schema = json.load(schema_file)

    requests = []
    for file in request_files:
        with open(request_dir + '/' + file) as request_file:
            try:
                request_obj = json.load(request_file)
                jsonschema.validate(request_obj, schema)
                if request_type is None:
                    requests.append(request_obj)
                elif request_obj['type'] == request_type:
                    requests.append(request_obj)
                else:
                    continue
            except (json.decoder.JSONDecodeError, jsonschema.exceptions.ValidationError):
                continue

    return requests


# retrieves a list of objects stored in a given s3 bucket (assumes permissions)
def get_objects_s3(bucket_name, region):
    s3_client = boto3.client('s3', region_name=region)

    s3_data = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
    keys = [obj["Key"] for obj in list(s3_data)][2:]

    widgets = []
    for key in keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        widget_str = response["Body"].read().decode("utf-8")
        widgets.append(json.loads(widget_str))
    return widgets


# retrieves a list of items stored in a given dynamodb table (assumes permissions)
def get_objects_dynamodb(table_name, region):
    dynamodb_client = boto3.client('dynamodb', region_name=region)
    paginator = dynamodb_client.get_paginator('scan')

    widgets = []
    for page in paginator.paginate(TableName=table_name):
        for item in page["Items"]:
            widget_obj = {}
            for attr in item.keys():
                widget_obj[attr] = item[attr]['S']
            widgets.append(widget_obj)
    return widgets


# flattens an object that may contain other objects into a single object
def flatten_obj(data_obj):
    data_flat = {}
    for key in data_obj.keys():
        if key == 'widgetId':
            data_flat['id'] = data_obj['widgetId']
        elif key == 'otherAttributes':
            for attr_pair in data_obj[key]:
                key = attr_pair['name']
                value = attr_pair['value']
                data_flat[key] = value
        else:
            data_flat[key] = data_obj[key]
    return data_flat


class TestConsumer(unittest.TestCase):

    # tests creating widget by comparing it against a schema
    def test_create_widget(self):
        requests = get_test_sample_requests(request_type='create')

        with open("./schemas/widget-schema.json") as schema_file:
            schema = json.load(schema_file)
            for request in requests:
                try:
                    widget = consumer.create_widget(logger, request)
                    jsonschema.validate(widget, schema)
                    valid_widget = True
                except jsonschema.exceptions.ValidationError:
                    valid_widget = False
                self.assertTrue(valid_widget)

    # tests updating a widget by comparing it against a schema
    def test_update_widget(self):
        requests = get_test_sample_requests(request_type='update')

        with open("./schemas/widget-schema.json") as schema_file:
            schema = json.load(schema_file)
            for request in requests:
                try:
                    widget = consumer.update_widget(logger, request)
                    jsonschema.validate(widget, schema)
                    valid_widget = True
                except jsonschema.exceptions.ValidationError:
                    valid_widget = False
                self.assertTrue(valid_widget)

    # tests deleting widgets from an s3 bucket based on delete requests
    def test_delete_widget_s3(self):
        delete_requests = get_test_sample_requests(request_type='delete')
        create_requests = get_test_sample_requests(request_type='create')
        widget_bucket = "usu-cs5250-coolmint-web"
        region = "us-east-1"

        # upload widgets to s3 bucket
        for cr_request in create_requests:
            widget = consumer.create_widget(logger, cr_request)
            consumer.save_to_s3(logger, widget, widget_bucket, region)

        # delete widgets
        deleted_widget_ids = []
        for dt_request in delete_requests:
            deleted_widget_ids.append(dt_request["widgetId"])
            consumer.delete_widget_s3(logger, dt_request, widget_bucket, region)

        # get objects in the s3 bucket
        s3_widgets = get_objects_s3(widget_bucket, region)
        s3_widget_ids = [obj['widgetId'] for obj in s3_widgets]

        # assure that no widget deleted is found in the s3 bucket
        for widget_id in deleted_widget_ids:
            self.assertNotIn(widget_id, s3_widget_ids)

    # tests deleting widgets from a dynamodb table based on delete requests
    def test_delete_widget_dynamodb(self):
        delete_requests = get_test_sample_requests(request_type='delete')
        create_requests = get_test_sample_requests(request_type='create')
        table_name = "widgets"
        region = "us-east-1"

        # upload widgets to dynamodb table
        for cr_request in create_requests:
            widget = consumer.create_widget(logger, cr_request)
            consumer.save_to_dynamodb(logger, widget, table_name, region)

        # delete widgets
        deleted_widget_ids = []
        for dt_request in delete_requests:
            deleted_widget_ids.append(dt_request["widgetId"])
            consumer.delete_widget_dynamodb(logger, dt_request, table_name, region)

        # get objects in the dynamodb table
        dynamodb_widgets = get_objects_dynamodb(table_name, region)
        dynamodb_widget_ids = [obj['id'] for obj in dynamodb_widgets]

        # assure that no widget deleted is found in the dynamodb table
        for widget_id in deleted_widget_ids:
            self.assertNotIn(widget_id, dynamodb_widget_ids)

    # tests retrieval of requests from a s3 bucket
    def test_get_request_s3(self):
        requests = get_test_sample_requests()
        request_bucket = "usu-cs5250-coolmint-requests"
        region = "us-east-1"
        request_loc = {
            "REQUEST_QUEUE": None,
            "REQUEST_BUCKET": request_bucket
        }

        s3_client = boto3.client('s3', region_name=region)
        for request in requests:
            request_str = json.dumps(request)
            s3_client.put_object(Bucket=request_bucket, Key=request['requestId'], Body=request_str)

        request = consumer.get_request_s3(logger, request_bucket, region)
        s3_request_ids = []
        while request is not None:
            s3_request_ids.append(request['requestId'])
            consumer.delete_request(logger, request, request_loc, region)
            request = consumer.get_request_s3(logger, request_bucket, region)

        for local_request in requests:
            local_request_id = local_request['requestId']
            self.assertIn(local_request_id, s3_request_ids)

    # tests retrieval of requests from sqs queue
    def test_get_request_sqs(self):
        requests = get_test_sample_requests()
        sqs_queue = "https://sqs.us-east-1.amazonaws.com/767843770882/cs5250-requests"
        region = "us-east-1"
        request_loc = {
            "REQUEST_QUEUE": sqs_queue,
            "REQUEST_BUCKET": None
        }

        sqs_client = boto3.client('sqs', region_name=region)
        for request in requests:
            request_str = json.dumps(request)
            sqs_client.send_message(QueueUrl=sqs_queue, MessageBody=request_str)

        request = consumer.get_request_sqs(logger, sqs_queue, region)
        sqs_request_ids = []
        while request is not None:
            sqs_request_ids.append(request['requestId'])
            consumer.delete_request(logger, request, request_loc, region)
            request = consumer.get_request_sqs(logger, sqs_queue, region)

        for local_request in requests:
            local_request_id = local_request['requestId']
            self.assertIn(local_request_id, sqs_request_ids)

    # tests the saving of widgets into a s3 bucket
    def test_save_to_s3(self):
        requests = get_test_sample_requests(request_type='create')
        bucket_name = "usu-cs5250-coolmint-web"
        region = "us-east-1"

        widgets = []
        for request in requests:
            widget_obj = consumer.create_widget(logger, request)
            consumer.save_to_s3(logger, widget_obj, bucket_name, region)
            widgets.append(widget_obj)

        s3_widgets = get_objects_s3(bucket_name, region)

        for widget in widgets:
            self.assertIn(widget, s3_widgets)

    # tests the saving of widgets into a dynamodb table
    def test_save_to_dynamodb(self):
        requests = get_test_sample_requests(request_type='create')
        table_name = "widgets"
        region = "us-east-1"

        widgets = []
        for request in requests:
            widget = consumer.create_widget(logger, request)
            consumer.save_to_dynamodb(logger, widget, table_name, region)
            widgets.append(flatten_obj(widget))

        dynamodb_widgets = get_objects_dynamodb(table_name, region)

        for widget in widgets:
            self.assertIn(widget, dynamodb_widgets)


if __name__ == "__main__":
    unittest.main()
