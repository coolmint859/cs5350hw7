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

    logger.info("Created Widget")
    return widget_obj


def delete_widget(logger, widget_data):
    logger.info("Deleted Widget")
    return widget_data


def update_widget(logger, widget_data):
    logger.info("Updated Widget")
    return widget_data


def get_next_widget(logger, s3_client, bucket_name, download_path):
    requests = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")

    if requests is None:
        return None, None

    request_keys = [obj["Key"] for obj in list(requests)]
    min_key = min(request_keys)

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    s3_client.download_file(bucket_name, min_key, download_path + min_key)
    logger.info(f"Loaded object with key '{min_key}' from bucket '{bucket_name}'")

    with open(download_path + min_key) as dnl_file:
        widget = json.load(dnl_file)

    return widget, min_key


def push_widget(logger, widget_obj):
    bucket_owner = widget_obj['owner'].replace(" ", "-").lower()
    bucket_id = widget_obj['widgetId']
    widget_path = f"sample_responses/{bucket_owner}/"

    if not os.path.exists(widget_path):
        os.makedirs(widget_path)

    with open(widget_path + f"{bucket_id}.json", 'w') as widget_file:
        json.dump(widget_obj, widget_file)


def widget_contains_required_keys(logger, widget_data):
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in widget_data:
                logger.warning(f"Widget Missing Required Key '{key}', Skipping...")
                return False
    return True


def process_widget(logger, widget):
    if not widget_contains_required_keys(logger, widget):
        return

    accepted_types = {
        "create": create_widget,
        "delete": delete_widget,
        "update": update_widget
    }

    if widget['type'] in accepted_types:
        widget_obj = accepted_types[widget['type']](logger, widget)
        push_widget(logger, widget_obj)
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

    bucket_name = "usu-cs5250-coolmint-requests"
    download_path = "downloaded-requests/"

    max_wait_time = 3  # wait time without finding a request before program terminates
    curr_wait_time = 0

    while curr_wait_time <= max_wait_time:
        widget, key = get_next_widget(logger, s3, bucket_name, download_path)
        if widget is not None:
            print(widget)
            # process_widget(logger, widget)
            s3.delete_object(Bucket=bucket_name, Key=key)
            curr_wait_time = 0
        else:
            # print(curr_wait_time)
            time.sleep(0.1)
            curr_wait_time += 0.1

    logger.info(f"No new requests found within the last {max_wait_time} seconds, terminating programs.")


if __name__ == "__main__":
    main()