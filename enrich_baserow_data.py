import requests
import subprocess
import os
from config import br_client, BASEROW_DB_ID
from acdh_geonames_utils.gn_client import gn_as_object
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri


place_table_id = br_client.get_table_by_name(BASEROW_DB_ID, "places")
something_was_updated = False

def get_filters_for_request():
    geoname_field_id = "field_23572"
    lat_field_id = "field_23583"
    return {
        f"filter__{geoname_field_id}__contains": "www.geonames",
        f"filter__{geoname_field_id}__contains": "https", 
        f"filter__{lat_field_id}__empty": True
    }


def get_items_to_update() -> list:
    table_items = []
    request_filters = get_filters_for_request()
    for item in br_client.yield_rows(place_table_id, filters=request_filters):
        table_items.append(item)
    return table_items


def update_lat_and_long(item: dict, update_data: dict):
    if not item["lat"].strip() or not item["long"].strip():
        gn_object = gn_as_object(item["geonames"])
        update_data["lat"] = gn_object["latitude"]
        update_data["long"] = gn_object["longitude"]
    return update_data


def update_geonames_uri(item: dict, update_data: dict):
    current_geoname_uri = item["geonames"]
    normalized_geoname_uri = get_normalized_uri(current_geoname_uri)
    if normalized_geoname_uri != current_geoname_uri:
        update_data["geonames"] = normalized_geoname_uri
    return update_data


def get_update_for_item(item:dict):
    update_data = {}
    update_data = update_lat_and_long(item, update_data)
    update_data = update_geonames_uri(item, update_data)
    return update_data


def update_item_online(item:dict, update_data:dict):
    global something_was_updated
    if something_was_updated is False:
        something_was_updated = True
    update_target_url = f"{br_client.br_base_url}database/rows/table/{place_table_id}/{item['id']}/?user_field_names=true"
    result = requests.patch(
        update_target_url,
        headers={
            "Authorization": f"Token {br_client.br_token}",
            "Content-Type": "application/json",
        },
        json=update_data,
    )
    return result


def update_coordinates_and_geoname_uris():
    # I dont use filter in the request, because 
    # I can't use them to check if the geonames uri is canonical
    # see https://github.com/acdh-oeaw/arche-assets#python 
    for item in br_client.yield_rows(place_table_id):
        update_data = get_update_for_item(item)
        if update_data:
            update_item_online(item, update_data)

if __name__ == "__main__":
    update_coordinates_and_geoname_uris()
    if something_was_updated:
        subprocess.run(["touch", f"{os.environ['BASEROW_STATUS_FILENAME']}"])