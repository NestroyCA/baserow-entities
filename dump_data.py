import os
import json
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri
from config import br_client, BASEROW_DB_ID, JSON_FOLDER

os.makedirs(JSON_FOLDER, exist_ok=True)

def modify_dump(json_file_path: str, fieldnames_to_manipulations: dict):
    """
    Loads json file from json_file_path and performs a set of manipulations.
    fieldnames_to_manipulations contains a set of fieldnames-strings as keys, 
    defining the set of fields to be manipulated, and as corresponding value
    a function performing the desired manipulations
    """
    print(f"updating {', '.join(fieldnames_to_manipulations.keys())} in {json_file_path}")
    # load data
    json_data = None
    with open(json_file_path, "r") as json_file_io:
        json_data = json.load(json_file_io)
    # apply changes
    for entity_id in json_data.keys():
        entity = json_data[entity_id]
        for fieldname in fieldnames_to_manipulations:
            manipulation_function = fieldnames_to_manipulations[fieldname]
            current_field_value = entity[fieldname]
            new_field_value = manipulation_function(current_field_value)
            entity[fieldname] = new_field_value
        json_data[entity_id] = entity
    # dump data
    with open(json_file_path, "w") as outfile:
        json.dump(json_data, outfile, indent=2)


json_file_paths = br_client.dump_tables_as_json(BASEROW_DB_ID, folder_name="json_dumps", indent=2)
places_filepath = "json_dumps/places.json"
if os.path.isfile(places_filepath):
    fieldnames_to_manipulations = {
        "geonames" : get_normalized_uri,
    }
    modify_dump(
        places_filepath,
        fieldnames_to_manipulations
    )
else:
    print(f"Missing '{places_filepath}'; was something renamed?")
    raise FileNotFoundError

for path in json_file_paths:
    print(path)