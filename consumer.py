import boto3
import click
import time
import json
import logging


def create_widget(logger, widget_data: dict[str: str]) -> dict[str: str]:
    widget_obj = {"widgetId": widget_data["widgetId"],
                  "owner": widget_data["owner"],
                  "description": widget_data["description"]}

    if 'otherAttributes' in widget_data.keys():
        widget_obj["otherAttributes"] = widget_data["otherAttributes"]

    if 'label' in widget_data.keys():
        widget_obj["label"] = widget_data["label"]

    logger.info(f"Created New Widget {widget_obj['widgetId']}")
    return widget_obj


def delete_widget(logger, widget_data: dict[str: str]):
    logger.info(f"Deleted Widget")

    return widget_data


def update_widget(logger, widget_data: dict[str: str]):
    logger.info(f"Updated Widget")
    return widget_data


def get_next_request(logger, s3_client, bucket_name: str) -> (dict[str: str], int):
    requests = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")

    if requests is None:
        return None, None

    request_keys = [obj["Key"] for obj in list(requests)]
    min_key = min(request_keys)

    response = s3_client.get_object(Bucket=bucket_name, Key=min_key)
    object_content = response["Body"].read().decode("utf-8")
    widget = json.loads(object_content)

    logger.debug(f"Retrieved request {widget['widgetId']}")

    return widget, min_key


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


def request_contains_required_keys(logger, widget_data: dict[str: str]) -> bool:
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in widget_data:
                logger.warning(f"Widget Missing Required Key '{key}', Skipping...")
                return False
    return True


def process_request(logger, request: dict[str: str]) -> dict[str: str]:
    if not request_contains_required_keys(logger, request):
        return

    accepted_types = {
        "create": create_widget,
        "delete": delete_widget,
        "update": update_widget
    }

    if request['type'] in accepted_types:
        widget_obj = accepted_types[request['type']](logger, request)
        return widget_obj
    else:
        logger.warning(f"Widget Type '{request['type']}' is an Invalid Type, Skipping...")
        return None


def create_logger(debug) -> logging.Logger:
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
        level=logging.DEBUG if debug else logging.INFO)

    return logger


def main(user_info: dict[str: str]) -> None:
    logger = create_logger(user_info["DEBUG"])

    s3 = boto3.client('s3')
    curr_wait_time = 0

    while curr_wait_time <= user_info["MAX_WAIT_TIME"]:
        request, key = get_next_request(logger, s3, user_info["REQUEST_BUCKET"])
        if request is not None:
            widget = process_request(logger, request)

            if widget is None:
                continue

            if "WIDGET_BUCKET" in user_info.keys():
                save_to_s3(logger, widget, user_info["WIDGET_BUCKET"])
            else:
                save_to_dynamodb(logger, widget, user_info["DYNAMODB_TABLE"])


            s3.delete_object(Bucket=user_info["REQUEST_BUCKET"], Key=key)
            logger.debug(f"Fulfilled request '{key}', finding next request...\n")
            curr_wait_time = 0
        else:
            time.sleep(0.1)
            curr_wait_time += 0.1

    logger.info(f"No new requests found within the last {user_info['MAX_WAIT_TIME']} seconds, terminating program.")



@click.group(invoke_without_command=True)
@click.option("--request-bucket", "-rb", help="Name of s3 bucket that may contain requests.", required=True)
@click.option("--widget-bucket", "-wb", help="Name of the s3 bucket that may contain widgets.")
@click.option("--dynamodb-table", "-dbt", help="Name of the dynamodb table that may contain widgets.")
@click.option("--max-wait-time", "-mwt", default=3, help="The max wait time in seconds without finding a request")
@click.option("--debug/--no-debug", default=False, help="If set, will print information for each step of the process.")
def cli(request_bucket, widget_bucket, dynamodb_table, max_wait_time, debug):
    if widget_bucket and dynamodb_table:
        click.echo("Error, Mismatched Options. To see possible parameters, type '--help'.")
        return
    if not (widget_bucket or dynamodb_table):
        click.echo("Error, Missing the Widget Location. To see more information, type '--help'.")
        return

    user_info = {'REQUEST_LOC': 's3',
                 'REQUEST_BUCKET': request_bucket}

    if widget_bucket is not None:
        user_info['WIDGET_BUCKET'] = widget_bucket
    else:
        user_info['DYNAMODB_TABLE'] = dynamodb_table

    user_info["MAX_WAIT_TIME"] = max_wait_time
    user_info["DEBUG"] = debug

    main(user_info)


if __name__ == "__main__":
    cli()
