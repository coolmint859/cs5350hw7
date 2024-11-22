import boto3
import sys
import os
import time
import json
import logging


def create_widget(logger, widget_data):
    widget_obj = {"widgetId": widget_data["widgetId"],
                  "owner": widget_data["owner"],
                  "description": widget_data["description"]}

    if 'otherAttributes' in widget_data.keys():
        widget_obj["otherAttributes"] = widget_data["otherAttributes"]

    if 'label' in widget_data.keys():
        widget_obj["label"] = widget_data["label"]

    logger.info(f"Created New Widget {widget_obj['widgetId']}")
    return widget_obj


def delete_widget(logger, widget_data):
    # logger.info(f"Deleted Widget {widget_obj['widgetID']}")
    logger.info(f"Deleted Widget")

    return widget_data


def update_widget(logger, widget_data):
    # logger.info(f"Created New Widget {widget_obj['widgetID']}")
    logger.info(f"Updated Widget")
    return widget_data


def get_next_widget(logger, s3_client, bucket_name):
    requests = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")

    if requests is None:
        return None, None

    request_keys = [obj["Key"] for obj in list(requests)]
    min_key = min(request_keys)

    response = s3_client.get_object(Bucket=bucket_name, Key=min_key)
    object_content = response["Body"].read().decode("utf-8")
    widget = json.loads(object_content)

    return widget, min_key


def push_widget(logger, s3_client, widget_obj, bucket_name):
    bucket_owner = widget_obj['owner'].replace(" ", "-").lower()

    widget_path = f"widgets/{bucket_owner}/"

    widget = json.dumps(widget_obj)
    s3_client.put_object(Bucket=bucket_name, Key=widget_path, Body=widget)

    # logger.info(f"Uploaded widget in {widget_path} as {widget_obj['widgetId']}.json")


def widget_contains_required_keys(logger, widget_data):
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in widget_data:
                logger.warning(f"Widget Missing Required Key '{key}', Skipping...")
                return False
    return True


def process_widget(logger, s3_client, widget, widget_bucket_name):
    if not widget_contains_required_keys(logger, widget):
        return

    accepted_types = {
        "create": create_widget,
        "delete": delete_widget,
        "update": update_widget
    }

    if widget['type'] in accepted_types:
        widget_obj = accepted_types[widget['type']](logger, widget)
        push_widget(logger, s3_client, widget_obj, widget_bucket_name)
    else:
        logger.warning(f"Widget Type '{widget['type']}' is an Invalid Type, Skipping...")


def create_logger():
    logger = logging.getLogger(__name__)
    logformatter = logging.Formatter("{asctime} {levelname} {name}: {message}", style='{')
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logformatter)
    logger.addHandler(consoleHandler)

    logging.basicConfig(
        filename="consumer.log",
        filemode='w',
        format="{asctime} {levelname} {name}: {message}",
        style="{",
        datefmt="%H:%M:%S",
        level=logging.INFO)

    return logger


def main():
    logger = create_logger()

    s3 = boto3.client('s3')

    request_bucket_name = "usu-cs5250-coolmint-requests"
    widget_bucket_name = "usu-cs5250-coolmint-web"

    max_wait_time = 3  # wait time without finding a request before program terminates
    curr_wait_time = 0

    while curr_wait_time <= max_wait_time:
        widget, key = get_next_widget(logger, s3, request_bucket_name)
        if widget is not None:
            # print(type(widget), widget)
            process_widget(logger, s3, widget, widget_bucket_name)
            s3.delete_object(Bucket=request_bucket_name, Key=key)
            curr_wait_time = 0
        else:
            time.sleep(0.1)
            curr_wait_time += 0.1

    logger.info(f"No new requests found within the last {max_wait_time} seconds, terminating program.")


if __name__ == "__main__":
    main()
