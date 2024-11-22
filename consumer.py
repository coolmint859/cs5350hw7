import boto3
import click
import time
import json
import logging


@click.group()
def commands():
    pass


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
    logger.info(f"Deleted Widget")

    return widget_data


def update_widget(logger, widget_data):
    logger.info(f"Updated Widget")
    return widget_data


def get_next_request(logger, s3_client, bucket_name):
    requests = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")

    if requests is None:
        return None, None

    request_keys = [obj["Key"] for obj in list(requests)]
    min_key = min(request_keys)

    response = s3_client.get_object(Bucket=bucket_name, Key=min_key)
    object_content = response["Body"].read().decode("utf-8")
    widget = json.loads(object_content)

    return widget, min_key


def push_widget_s3(logger, s3_client, widget_obj, bucket_name):
    bucket_owner = widget_obj['owner'].replace(" ", "-").lower()

    widget_path = f"widgets/{bucket_owner}/"

    widget = json.dumps(widget_obj)
    s3_client.put_object(Bucket=bucket_name, Key=widget_path, Body=widget)

    logger.info(f"Uploaded widget in {widget_path} as {widget_obj['widgetId']}.json")


def push_widget_dynamodb(logger, db_client, widget_obk, table_name):
    pass


def request_contains_required_keys(logger, widget_data):
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in widget_data:
                logger.warning(f"Widget Missing Required Key '{key}', Skipping...")
                return False
    return True


def process_request(logger, request):
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


@click.command()
@click.option("--request-bucket", "-rb", help="The bucket to pull requests from.")
@click.option("--dynamodb-table", "-dbt", help="The dynamodb table to fulfill requests on.")
@click.option("--max-wait-time", "-mwt", default=3, help="The max wait time in seconds without finding a request")
def save_to_dynamodb(request_bucket: str, dynamodb_table: str, max_wait_time: int):
    if request_bucket is None:
        click.echo("Missing request-bucket option. type --help to see usage.")
        return
    elif dynamodb_table is None:
        click.echo("Missing dynamodb-table option. type --help to see usage.")
        return

    click.echo(f"request-bucket={request_bucket}, dynamodb-table={dynamodb_table}, wait-time={max_wait_time}")


@click.command()
@click.option("--request-bucket", "-rb", help="The bucket to pull requests from.")
@click.option("--widget-bucket", "-wb", help="The bucket to fulfill requests on.")
@click.option("--max-wait-time", "-mwt", default=3, help="The max wait time in seconds before finding a request")
def save_to_s3(request_bucket: str, widget_bucket: str, max_wait_time: int):
    if request_bucket is None:
        click.echo("Missing request-bucket option. type --help to see usage.")
        return
    elif widget_bucket is None:
        click.echo("Missing widget-bucket option. type --help to see usage.")
        return

    logger = create_logger()

    s3 = boto3.client('s3')
    curr_wait_time = 0

    while curr_wait_time <= max_wait_time:
        request, key = get_next_request(logger, s3, request_bucket)
        if request is not None:
            widget = process_request(logger, request)

            if widget is None:
                continue

            push_widget_s3(logger, s3, widget, widget_bucket)

            s3.delete_object(Bucket=request_bucket, Key=key)
            logger.info(f"Fulfilled request '{key}', finding next request...")
            curr_wait_time = 0
        else:
            time.sleep(0.1)
            curr_wait_time += 0.1

    logger.info(f"No new requests found within the last {max_wait_time} seconds, terminating program.")


commands.add_command(save_to_dynamodb)
commands.add_command(save_to_s3)


if __name__ == "__main__":
    commands()
