import unittest
import os
from src import consumer
import json
import boto3


def get_test_sample_requests():
    request_files = os.listdir("sample-requests")

    requests = []
    for file in request_files:
        with open("sample-requests/" + file) as request_file:
            try:
                request_obj = json.load(request_file)
                requests.append(request_obj)
            except json.decoder.JSONDecodeError:
                continue
    return requests


def get_objects_s3(bucket_name):
    s3_client = boto3.client('s3')

    s3_data = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
    keys = [obj["Key"] for obj in list(s3_data)][2:]

    widgets = []
    for key in keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        obj = response["Body"].read().decode("utf-8")
        widgets.append(json.loads(obj))

    return widgets


def get_objects_dynamodb(table_name):
    dynamodb_client = boto3.client('dynamodb')
    paginator = dynamodb_client.get_paginator('scan')

    widgets = []
    for page in paginator.paginate(TableName=table_name):
        for item in page["Items"]:
            widget = {}
            for key in item.keys():
                widget[key] = item[key]['S']
            widgets.append(widget)
    return widgets


def flatten_obj(data_obj):
    data_flat = {}
    for key in data_obj.keys():
        if key == 'widgetId':
            data_flat['id'] = data_obj['widgetId']
            continue
        if key != 'otherAttributes':
            data_flat[key] = data_obj[key]
        else:
            for table in data_obj[key]:
                for key_inner in table:
                    data_flat[key_inner] = table[key_inner]
    return data_flat


class TestConsumer(unittest.TestCase):

    def test_create_widget(self):
        logger = consumer.create_logger(True, "logs/test_consumer.log")
        requests = get_test_sample_requests()

        for request in requests:

            expected_widget = {
                "widgetId": request["widgetId"],
                "owner": request["owner"],
                "description": request["description"],
                "label": request["label"],
                "otherAttributes": request["otherAttributes"]
                 }

            widget = consumer.create_widget(logger, request)
            self.assertEqual(widget, expected_widget)


    def test_delete_widget_s3(self):
        # logger = consumer.create_logger(True, "logs/test_consumer.log")
        # requests = get_test_sample_requests()
        #
        # for request in requests:
        #     widget = consumer.create_widget(logger, request)
        pass

    def test_update_widget(self):
        pass

    def test_save_to_s3(self):
        logger = consumer.create_logger(True, "logs/test_consumer.log")
        requests = get_test_sample_requests()
        bucket_name = "usu-cs5250-coolmint-web"

        widgets = []
        for request in requests:
            if consumer.is_valid_request(logger, request):
                widget = consumer.create_widget(logger, request)
                consumer.save_to_s3(logger, widget, bucket_name)
                widgets.append(widget)

        s3_widgets = get_objects_s3(bucket_name)

        for widget in widgets:
            self.assertIn(widget, s3_widgets)


    def test_save_to_dynamodb(self):
        logger = consumer.create_logger(True, "logs/test_consumer.log")
        requests = get_test_sample_requests()
        table_name = "widgets"

        widgets = []
        for request in requests:
            if not consumer.is_valid_request(logger, request):
                continue
            widget = consumer.create_widget(logger, request)
            consumer.save_to_dynamodb(logger, widget, table_name)
            widgets.append(flatten_obj(widget))

        dynamodb_widgets = get_objects_dynamodb(table_name)

        for widget in widgets:
            self.assertIn(widget, dynamodb_widgets)


if __name__ == "__main__":
    unittest.main()
