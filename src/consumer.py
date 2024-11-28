import boto3
import click
import time
import json
import logging


def create_widget(logger, request_data: dict[str: str]) -> dict[str: str]:
    widget_obj = {"widgetId": request_data["widgetId"],
                  "owner": request_data["owner"],
                  "description": request_data["description"]}

    if 'otherAttributes' in request_data.keys():
        widget_obj["otherAttributes"] = request_data["otherAttributes"]

    if 'label' in request_data.keys():
        widget_obj["label"] = request_data["label"]

    logger.info(f"Created Widget {widget_obj['widgetId']}")
    return widget_obj


def delete_widget(logger, request_data: dict[str: str], widget_loc: dict[str: str]) -> None:
    # logger.info("Deleted Widget")
    if widget_loc["WIDGET_BUCKET"]:
        # widget is in an s3 bucket
        s3_client = boto3.client('s3')

        bucket_owner = request_data['owner'].replace(" ", "-").lower()
        widget_path = f"widgets/{bucket_owner}/"
        bucket_name = widget_loc["WIDGET_BUCKET"]

        try:
            s3_client.get_object(Bucket=bucket_name, Key=widget_path)
        except s3_client.exceptions.NoSuchKey:
            logger.warning(f"Could not delete widget '{request_data['widgetId']}', widget does not exist.")
            return

        s3_client.delete_object(Bucket=bucket_name, Key=widget_path)
        logger.info(f"Deleted Widget {request_data['widgetId']}")
    else:
        # widget is in a dynamodb table
        pass



def update_widget(logger, request_data: dict[str: str]):
    new_widget = {"widgetId": request_data["widgetId"],
                  "owner": request_data["owner"],
                  "description": request_data["description"]}

    if 'otherAttributes' in request_data.keys():
        new_widget["otherAttributes"] = request_data["otherAttributes"]

    if 'label' in request_data.keys():
        new_widget["label"] = request_data["label"]

    logger.info(f"Updated Widget {new_widget['widgetId']}")
    return new_widget


def get_next_request(logger, user_info: dict[str: str]) -> (dict[str: str], int):
    if user_info["REQUEST_BUCKET"]:
        return get_request_s3(logger, user_info["REQUEST_BUCKET"])
    else:
        return get_request_sqs(logger, user_info["SQS_QUEUE"])


def get_request_s3(logger, bucket_name):
    s3_client = boto3.client('s3')

    # list_objects_v2 appears to always list in ascending order (likely the order the objects were uploaded),
    # so the first object in the list will always be the one with the smallest key
    requests = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1).get("Contents")

    if requests is None:
        return None

    request_key = list(requests)[0]["Key"]

    response = s3_client.get_object(Bucket=bucket_name, Key=request_key)
    object_content = response["Body"].read().decode("utf-8")

    request = json.loads(object_content)
    request['key'] = request_key

    logger.debug(f"Retrieved request {request['widgetId']}")

    return request


def get_request_sqs(logger, sqs_queue):
    widget = {"widgetID": "<Id goes here>"}
    logger.debug(f"Retrieved request {widget['widgetId']}")

    return None


def save_widget(logger, widget_obj, widget_loc):
    if widget_loc["WIDGET_BUCKET"]:
        save_to_s3(logger, widget_obj, widget_loc["WIDGET_BUCKET"])
    else:
        save_to_dynamodb(logger, widget_obj, widget_loc["DYNAMODB_TABLE"])


def save_to_s3(logger, widget_obj: dict[str: str], bucket_name: str) -> None:
    s3_client = boto3.client('s3')

    bucket_owner = widget_obj['owner'].replace(" ", "-").lower()

    widget_path = f"widgets/{bucket_owner}/"

    widget = json.dumps(widget_obj)
    s3_client.put_object(Bucket=bucket_name, Key=widget_path, Body=widget)

    logger.debug(f"Uploaded widget in {widget_path} as {widget_obj['widgetId']}.json")


def save_to_dynamodb(logger, widget_obj: dict[str: str], table_name: str) -> None:
    dynamodb_client = boto3.client("dynamodb")

    item_dict = {"id": {'S': widget_obj["widgetId"]}}

    for attr in widget_obj.keys():
        if attr == "otherAttributes" or attr == "widgetId":
            continue
        item_dict[attr] = {"S": widget_obj[attr]}

    if "otherAttributes" in widget_obj.keys():
        for table in widget_obj["otherAttributes"]:
            for attr in table:
                item_dict[attr] = {"S": table[attr]}

    dynamodb_client.put_item(TableName=table_name, Item=item_dict)
    logger.debug(f"Uploaded widget in '{table_name}' table as {widget_obj['widgetId']}")


def delete_request(logger, request_data: dict[str: str], request_loc: dict["str": str]) -> None:
    if request_loc["REQUEST_BUCKET"]:
        bucket = request_loc["REQUEST_BUCKET"]
        key = request_data["key"]

        # print(bucket, key)

        s3_client = boto3.client('s3')
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.debug(f"Deleted Request {request_data['requestId']}")
    else:
        sqs_client = boto3.client('sqs')


def process_request(logger, request: dict[str: str], user_info: dict["str": "str"]) -> None:
    if request['type'] == 'create':
        widget = create_widget(logger, request)
        save_widget(logger, widget, user_info["WIDGET_LOC"])
        delete_request(logger, request, user_info["REQUEST_LOC"])

    elif request['type'] == 'update':
        widget = update_widget(logger, request)
        save_widget(logger, widget, user_info["WIDGET_LOC"])
        delete_request(logger, request, user_info["REQUEST_LOC"])

    elif request['type'] == 'delete':
        delete_widget(logger, request, user_info["WIDGET_LOC"])
        delete_request(logger, request, user_info["REQUEST_LOC"])

    else:
        logger.warning(f"Widget Type '{request['type']}' is an Invalid Type, Skipping...")



def is_valid_request(logger, request: dict[str: str]) -> bool:
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in request:
                logger.warning(f"Request Missing '{key}', Skipping...")
                return False
    return True


def create_logger(debug, save_file) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logformatter = logging.Formatter("{asctime} {levelname} {name}: {message}", style='{')
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logformatter)
    logger.addHandler(consoleHandler)

    logging.basicConfig(
        filename=save_file,
        filemode='w',
        format="{asctime} {levelname} {name}: {message}",
        style="{",
        datefmt="%H:%M:%S",
        level=logging.DEBUG if debug else logging.INFO)

    return logger


def main(user_info: dict[str: str]) -> None:
    logger = create_logger(user_info["DEBUG"], "logs/consumer.log")

    curr_wait_time = 0
    while curr_wait_time <= user_info["MAX_WAIT_TIME"]:
        request = get_next_request(logger, user_info["REQUEST_LOC"])
        if request is not None:
            if not is_valid_request(logger, request):
                print("Invalid", type(request), request)
                continue

            process_request(logger, request, user_info)

            logger.debug(f"Fulfilled request '{request['requestId']}', finding next request...\n")
            curr_wait_time = 0
        else:
            time.sleep(0.1)
            curr_wait_time += 0.1

    logger.info(f"No new requests found within the last {user_info['MAX_WAIT_TIME']} seconds, terminating program.")



@click.group(invoke_without_command=True)
@click.option("--request-bucket", "-rb", help="Name of the s3 bucket that may contain requests.")
@click.option("--request-queue", "-rq", help="URL of the SQS queue that may contain requests.")
@click.option("--widget-bucket", "-wb", help="Name of the s3 bucket that may contain widgets.")
@click.option("--dynamodb-table", "-dbt", help="Name of the dynamodb table that may contain widgets.")
@click.option("--max-wait-time", "-mwt", default=3, help="The max wait time in seconds without finding a request")
@click.option("--debug/--no-debug", default=False, help="If set, will print information about fetching and processing requests.")
def cli(request_bucket, request_queue, widget_bucket, dynamodb_table, max_wait_time, debug):
    if (widget_bucket and dynamodb_table) or (request_bucket and request_queue):
        logging.error("Mismatched Options. To see more information, type '--help'.")
        return
    if not (request_bucket or request_queue):
        logging.error("Missing the Request Location. To see more information, type '--help'.")
        return
    if not (widget_bucket or dynamodb_table):
        logging.error("Missing the Widget Location. To see more information, type '--help'.")
        return

    user_info = {
        "REQUEST_LOC": {
            "REQUEST_BUCKET": request_bucket,
            "REQUEST_QUEUE": request_queue
        },
        "WIDGET_LOC": {
            "WIDGET_BUCKET": widget_bucket,
            "DYNAMODB_TABLE": dynamodb_table
        },
        "MAX_WAIT_TIME": max_wait_time,
        "DEBUG": debug
    }

    main(user_info)


if __name__ == "__main__":
    cli()
