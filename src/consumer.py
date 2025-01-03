import boto3
import click
import json
import logging
import jsonschema


# creates a widget object from a request object. The request is assumed to be verified
def create_widget(logger, request_data: dict[str: str], log=True) -> dict[str: str]:
    widget_obj = {}
    for attr in request_data.keys():
        if attr == "requestId" or attr == "type":  # ignore request specific attributes
            continue
        widget_obj[attr] = request_data[attr]

    if log:
        logger.info(f"Created Widget '{widget_obj['widgetId']}'")

    return widget_obj


# wrapper method for deleting a widget from a specified location
def delete_widget(logger, request_data: dict[str: str], widget_loc: dict[str: str], region: str) -> None:
    if widget_loc["WIDGET_BUCKET"]:
        delete_widget_s3(logger, request_data, widget_loc["WIDGET_BUCKET"], region)
    else:
        delete_widget_dynamodb(logger, request_data, widget_loc["DYNAMODB_TABLE"], region)


# deletes a widget from an s3 bucket, if the widget exists
def delete_widget_s3(logger, request_data: dict[str: str], widget_bucket: str, region: str) -> None:
    s3_client = boto3.client('s3', region_name=region)

    bucket_owner = request_data['owner'].replace(" ", "-").lower()
    widget_path = f"widgets/{bucket_owner}/{request_data['widgetId']}"

    try:
        s3_client.get_object(Bucket=widget_bucket, Key=widget_path)
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"Widget '{widget_path}' does not exist in s3 bucket '{widget_bucket}'.")
        return

    s3_client.delete_object(Bucket=widget_bucket, Key=widget_path)
    logger.info(f"Deleted Widget '{request_data['widgetId']}'")


# deletes a widget from a dynamodb table, if the widget exists
def delete_widget_dynamodb(logger, request_data: dict[str: str], widget_table: str, region: str) -> None:
    dynamodb_client = boto3.client("dynamodb", region_name=region)
    widget_obj = create_widget(logger, request_data, log=False)

    item_key = {"id": {'S': widget_obj["widgetId"]}}

    response = dynamodb_client.get_item(TableName=widget_table, Key=item_key)
    if 'Item' not in response.keys():
        logger.warning(f"Could not delete widget '{request_data['widgetId']}', widget does not exist.")
        return

    dynamodb_client.delete_item(TableName=widget_table, Key=item_key)
    logger.info(f"Deleted Widget '{request_data['widgetId']}'")


# updates a widget from the given request
def update_widget(logger, request_data: dict[str: str]):
    new_widget = {}
    for attr in request_data.keys():
        if attr == "requestId" or attr == "type":  # ignore request specific attributes
            continue
        new_widget[attr] = request_data[attr]

    logger.info(f"Updated Widget '{new_widget['widgetId']}'")
    return new_widget


# wrapper method for retrieving requests
def get_next_request(logger, request_loc: dict[str: str], region: str) -> (dict[str: str], int):
    if request_loc["REQUEST_BUCKET"]:
        return get_request_s3(logger, request_loc["REQUEST_BUCKET"], region)
    else:
        return get_request_sqs(logger, request_loc["REQUEST_QUEUE"], region)


# retrieves a request from an s3 bucket. If no request is found, returns None
def get_request_s3(logger, bucket_name: str, region: str) -> dict[str: str]:
    s3_client = boto3.client('s3', region_name=region)

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

    logger.debug(f"Retrieved request '{request['widgetId']}' from s3 bucket '{bucket_name}'")

    return request


# retrieves a request from an sqs queue. If no request is found, returns None
def get_request_sqs(logger, sqs_queue: str, region: str) -> dict[str: str]:
    sqs_client = boto3.client('sqs', region_name=region)

    try:
        response = sqs_client.receive_message(QueueUrl=sqs_queue, VisibilityTimeout=5)
        if 'Messages' not in response:
            return None
        request_str = response["Messages"][0]["Body"]
        request = json.loads(request_str)

        request['receipt_handle'] = response["Messages"][0]["ReceiptHandle"]
    except sqs_client.exceptions.InvalidAddress:
        logger.warning(f"'{sqs_queue}' is an invalid URL, unable to retrieve requests.")
        return None
    except IndexError:
        return None

    logger.debug(f"Retrieved request '{request['widgetId']}' from SQS Queue '{sqs_queue}'")
    return request


# wrapper method for saving a widget
def save_widget(logger, widget_obj: dict[str: str], widget_loc: dict[str: str], region: str) -> None:
    if widget_loc["WIDGET_BUCKET"]:
        save_to_s3(logger, widget_obj, widget_loc["WIDGET_BUCKET"], region)
    else:
        save_to_dynamodb(logger, widget_obj, widget_loc["DYNAMODB_TABLE"], region)


# saves the given widget object into an s3 bucket. If the widget already exists, this method overwrites it.
def save_to_s3(logger, widget_obj: dict[str: str], bucket_name: str, region: str) -> None:
    s3_client = boto3.client('s3', region_name=region)

    bucket_owner = widget_obj['owner'].replace(" ", "-").lower()
    widget_path = f"widgets/{bucket_owner}/{widget_obj['widgetId']}"

    widget = json.dumps(widget_obj)
    s3_client.put_object(Bucket=bucket_name, Key=widget_path, Body=widget)

    logger.debug(f"Uploaded widget in s3 bucket '{bucket_name}' in '{widget_path}' as '{widget_obj['widgetId']}.json'")


# saves the given widget object into a dynamodb table. if the widget already exists, this method overwrites it.
def save_to_dynamodb(logger, widget_obj: dict[str: str], table_name: str, region: str) -> None:
    dynamodb_client = boto3.client("dynamodb", region_name=region)

    item_dict = {
        "id": {'S': widget_obj["widgetId"]}
    }

    for attr in widget_obj:
        if attr == "otherAttributes" or attr == "widgetId":
            continue
        item_dict[attr] = {"S": widget_obj[attr]}

    if "otherAttributes" in widget_obj:
        for attr_pair in widget_obj["otherAttributes"]:
            key = attr_pair['name']
            value = attr_pair['value']

            item_dict[key] = {"S": value}

    dynamodb_client.put_item(TableName=table_name, Item=item_dict)
    logger.debug(f"Uploaded widget in '{table_name}' table as {widget_obj['widgetId']}")


# deletes a request from an s3 queue or SQS Queue.
def delete_request(logger, request_data: dict[str: str], request_loc: dict["str": str], region: str) -> None:
    if request_loc["REQUEST_BUCKET"]:
        bucket = request_loc["REQUEST_BUCKET"]
        key = request_data["key"]

        s3_client = boto3.client('s3', region_name=region)
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.debug(f"Deleted Request {request_data['requestId']} from s3 bucket {bucket}.")
    else:
        sqs_client = boto3.client('sqs', region_name=region)
        queue_url = request_loc['REQUEST_QUEUE']
        request_key = request_data['receipt_handle']

        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=request_key)
        logger.debug(f"Deleted Request {request_data['requestId']} from sqs queue {queue_url}.")


# wrapper function that directs the program to do specific things depending on what type the request is
def process_request(logger, request: dict[str: str], user_info: dict["str": "str"], region: str) -> None:
    if request['type'] == 'create':
        widget = create_widget(logger, request)
        save_widget(logger, widget, user_info["WIDGET_LOC"], region)
        delete_request(logger, request, user_info["REQUEST_LOC"], region)

    elif request['type'] == 'update':
        widget = update_widget(logger, request)
        save_widget(logger, widget, user_info["WIDGET_LOC"], region)
        delete_request(logger, request, user_info["REQUEST_LOC"], region)

    elif request['type'] == 'delete':
        delete_widget(logger, request, user_info["WIDGET_LOC"], region)
        delete_request(logger, request, user_info["REQUEST_LOC"], region)

    else:
        logger.warning(f"Widget Type '{request['type']}' is an Invalid Type, Skipping...")


# validates a given request by comparing it to a json schema file
def is_valid_request(logger, request: dict[str: str]) -> bool:
    with open("./schemas/request-schema.json") as schema_file:
        schema = json.load(schema_file)
        try:
            jsonschema.validate(request, schema)
            logger.debug(f"Validated Request {request['requestId']}")
            return True
        except jsonschema.exceptions.ValidationError:
            logger.warning(f"Request {request['requestId']} could not be validated, skipping this request...")
            return False


# creates a logger object to log what the program is doing while processing requests
def create_logger(debug: bool, save_file: str) -> logging.Logger:
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


# continuously looks for requests in the given request location until no requests are found for a given number of times
def main(user_info: dict[str: str]) -> None:
    logger = create_logger(debug=user_info["DEBUG"], save_file="./logs/consumer.log")

    curr_failed_requests = 0
    while curr_failed_requests <= user_info["MAX_REQUEST_LIMIT"]:
        request = get_next_request(logger, user_info["REQUEST_LOC"], user_info["REGION"])
        if request is not None:
            if not is_valid_request(logger, request):
                continue

            process_request(logger, request, user_info, user_info["REGION"])

            logger.debug(f"Fulfilled request '{request['requestId']}'\n")
            curr_failed_requests = 0
        else:
            curr_failed_requests += 1

    logger.info(f"Max number of failed request polls reached, terminating program.")


# entry point of the program - packages the given user data into a dictionary to be used throughout the program
@click.group(invoke_without_command=True)
@click.option("--region", help="The region of the aws service instances")
@click.option("--request-bucket", "-rb", help="Name of the s3 bucket that may contain requests.")
@click.option("--request-queue", "-rq", help="URL of the SQS queue that may contain requests.")
@click.option("--widget-bucket", "-wb", help="Name of the s3 bucket that may contain widgets.")
@click.option("--dynamodb-table", "-dbt", help="Name of the dynamodb table that may contain widgets.")
@click.option("--max-request-limit", "-mrl", default=15,
              help="The max number of failed request polls before terminating")
@click.option("--debug/--no-debug", default=False,
              help="If set, will print information about fetching and processing requests.")
def cli(region, request_bucket, request_queue, widget_bucket, dynamodb_table, max_request_limit, debug):
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
        "MAX_REQUEST_LIMIT": max_request_limit,
        "DEBUG": debug,
        "REGION": region
    }

    main(user_info)


if __name__ == "__main__":
    cli()
