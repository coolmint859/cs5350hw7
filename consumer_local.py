import os
import json
import logging
import glob


def create_widget_obj(logger, widget_data):
    widget_obj = {"widgetId": widget_data["widgetId"],
                  "owner": widget_data["owner"],
                  "description": widget_data["description"]}

    if 'otherAttributes' in widget_data.keys():
        widget_obj["otherAttributes"] = widget_data["otherAttributes"]

    if 'label' in widget_data.keys():
        widget_obj["label"] = widget_data["label"]

    logger.info("Created Widget")
    return widget_obj


def delete_widget_obj(logger, widget_data):
    logger.info("Deleted Widget")
    return widget_data


def update_widget_obj(logger, widget_data):
    logger.info("Updated Widget")
    return widget_data


def get_widgets():
    widget_files = glob.glob("sample-requests/*")

    widgets = []
    for file in widget_files:
        with open(file) as widget_file:
            try:
                widget_obj = json.load(widget_file)
                widgets.append(widget_obj)
            except json.decoder.JSONDecodeError:
                continue
    return widgets


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
        "create": create_widget_obj,
        "delete": delete_widget_obj,
        "update": update_widget_obj
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
        level=logging.DEBUG)

    return logger


def main():
    logger = create_logger()

    widgets = get_widgets()
    for widget in widgets:
        process_widget(logger, widget)


if __name__ == "__main__":
    main()