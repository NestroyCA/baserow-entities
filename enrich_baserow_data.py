import requests
from config import br_client, BASEROW_DB_ID
from acdh_geonames_utils.gn_client import gn_as_object
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri

def get_coordinates_filters_for_request(geoname_field_id, lat_field_id):
    """filter to request items missing the coordinates"""
    return {
        f"filter__{geoname_field_id}__contains": "www.geonames",
        f"filter__{geoname_field_id}__contains": "https", 
        f"filter__{lat_field_id}__empty": True
    }

def get_items_to_update(request_filters, table_id) -> list:
    """get items from baserow according
    to filer, returns list"""
    table_items = []
    for item in br_client.yield_rows(table_id, filters=request_filters):
        table_items.append(item)
    return table_items


def update_lat_and_long(item: dict, update_data: dict):
    geonames_uri_exists = "http" in item['geonames'] if item["geonames"] is not None else False
    item_lat_exists = bool(item["lat"].strip()) if item["lat"] is not None else False
    item_long_exsits = bool(item["long"].strip()) if item["long"] is not None else False
    if geonames_uri_exists and (not item_lat_exists or not item_long_exsits):
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


def update_item_online(table_id:str, item_id: str, update_data:dict):
    update_target_url = f"{br_client.br_base_url}database/rows/table/{table_id}/{item_id}/?user_field_names=true"
    result = requests.patch(
        update_target_url,
        headers={
            "Authorization": f"Token {br_client.br_token}",
            "Content-Type": "application/json",
        },
        json=update_data,
    )
    return result


def update_table_with_coordinates(table_name, geoname_field_id, lat_field_id):
    table_id = br_client.get_table_by_name(BASEROW_DB_ID, table_name)
    lat_long_filter = get_coordinates_filters_for_request(geoname_field_id, lat_field_id)
    items_to_update = get_items_to_update(lat_long_filter, table_id)
    for item_to_update in items_to_update:
        item_to_update_id = item_to_update["id"]
        item_update = get_update_for_item(item_to_update)
        result = update_item_online(
            table_id = table_id,
            item_id = item_to_update_id,
            update_data = item_update
        )
        print(result)


if __name__ == "__main__":
    # update places table
    update_table_with_coordinates(
        table_name = "places",
        geoname_field_id = "field_23572",
        lat_field_id = "field_23583"
    )

    update_table_with_coordinates(
        table_name = "vienna_places",
        geoname_field_id = "field_23579",
        lat_field_id = "field_23746"
    )