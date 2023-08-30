import requests
from tqdm import tqdm
from config import br_client, BASEROW_DB_ID
from acdh_geonames_utils.gn_client import gn_as_object


place_table_id = br_client.get_table_by_name(BASEROW_DB_ID, "places")
geoname_field_id = "field_23572"
lat_field_id = "field_23583"
filters = {f"filter__{geoname_field_id}__contains": "www.geonames", f"filter__{lat_field_id}__empty": True}

def get_items_to_update(filters:str=filters) -> list:
    table_items = []
    for item in br_client.yield_rows(place_table_id, filters=filters):
        table_items.append(item)
    return table_items

def get_update_for_item(item:dict):
    update_data = {}
    gn_object = gn_as_object(x["geonames"])
    update_data["lat"] = gn_object["latitude"]
    update_data["long"] = gn_object["longitude"]
    return update_data

items_to_update = get_items_to_update()
for item in tqdm(items_to_update):
    update_data = get_update_for_item(item)
    update_target_url = f"{br_client.br_base_url}database/rows/table/{place_table_id}/{item['id']}/?user_field_names=true"
    r = requests.patch(
        update_target_url,
        headers={
            "Authorization": f"Token {br_client.br_token}",
            "Content-Type": "application/json",
        },
        json=update_data,
    )