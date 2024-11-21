import boto3
import sys
import time
import os
import json
import logging
import glob


def create_widget_obj(widget_data):
    print("creating widget!")

    widget_obj = {"widgetId": widget_data["widgetId"],
                  "owner": widget_data["owner"],
                  "description": widget_data["description"]}

    if 'otherAttributes' in widget_data.keys():
        widget_obj["otherAttributes"] = widget_data["otherAttributes"]

    if 'label' in widget_data.keys():
        widget_obj["label"] = widget_data["label"]

    return widget_obj


def delete_widget_obj(widget_data):
    print("delete widget!")
    return widget_data


def update_widget_obj(widget_data):
    print("update widget!")
    return widget_data


def bucketizeName(name):
    name = name.replace(" ", "-").lower()
    return name


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


def push_widget(widget_obj):
    widget_path = f"sample_responses/{bucketizeName(widget_obj['owner'])}/"

    if not os.path.exists(widget_path):
        os.makedirs(widget_path)

    with open(widget_path + f"{widget_obj['widgetId']}.json", 'w') as widget_file:
        json.dump(widget_obj, widget_file)
    print("widget created!")


def widget_contains_required_keys(widget_data):
    with open("widgetRequest-schema.json") as schema_file:
        schema = json.load(schema_file)

        for key in schema['required']:
            if key not in widget_data:
                return False
    return True


def process_widget(widget):
    if not widget_contains_required_keys(widget):
        print(f"{widget['requestId']} does not contain required keys!")
        return

    accepted_types = {
        "create": create_widget_obj,
        "delete": delete_widget_obj,
        "update": update_widget_obj
    }

    if widget['type'] in accepted_types:
        widget_obj = accepted_types[widget['type']](widget)
        push_widget(widget_obj)
    else:
        print(f"'{widget['type']}' is not a valid widget type!")


def main():
    max_wait_time = 5  # wait time 5 seconds
    curr_wait_time = 0


    widgets = get_widgets()
    # process_widget(widgets[0])
    for widget in widgets:
        print(widget.keys())
        process_widget(widget)

    # while curr_wait_time <= max_wait_time:
    #     widget = get_next_widget()
    #     if widget is not None:
    #         process_widget(widget)
    #         curr_wait_time = 0
    #     else:
    #         # print("zzzzzz...." + str(curr_wait_time))
    #         time.sleep(0.1)
    #         curr_wait_time += 0.1


if __name__ == "__main__":
    main()
