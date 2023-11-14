from copy import deepcopy
import os
import json
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri
from config import br_client, BASEROW_DB_ID, JSON_FOLDER

play_id_2_play_name = None
altname_keys = ["alt_tokens", "legacy"]

def make_tabulator_data_entry(
        name: str,
        lng: str,
        lat: str,
        geonames_url: str,
        internal_id: str,
        mentions: list,
        alt_names: list,
        total_occurences: int
    ):
    return {
        "coordinates": {
        "lng": lng,
        "lat": lat
        },
        "name": name,
        "geonames": [
            "geonames",
            geonames_url
        ],
        "internal_id": internal_id,
        "mentions": mentions if mentions else [],
        "alt_names": alt_names,
        "total_occurences": total_occurences
    }


def create_tabulator_data(
        json_file_path:str,
        name_key: str,
        lng_key: str,
        lat_key: str,
        geonames_url_key: str,
        internal_id_key: str,
        mentions_key: str,
        altnames_keys: list,
        total_occurences_keys: str
    ):
    tabulator_data_output_path = f"{json_file_path.removesuffix('.json')}_tabulator.json"
    tabulator_data = []
    with open(json_file_path, "r") as json_file_io:
        json_data = json.load(json_file_io)
        for row in json_data.values():
            print(row)
            new_row = make_tabulator_data_entry(
                name=row[name_key],
                lng=row[lat_key],
                lat=row[lng_key],
                geonames_url=row[geonames_url_key],
                internal_id=row[internal_id_key],
                mentions=row[mentions_key],
                alt_names=[row[altnames_key] for altnames_key in altnames_keys if row[altnames_key]],
                total_occurences=row[total_occurences_keys] if row[total_occurences_keys] else 1
            )
            tabulator_data.append(new_row)
    with open(tabulator_data_output_path, "w") as tabulator_data_dumpfile:
        json.dump(
            tabulator_data,
            fp=tabulator_data_dumpfile,
            indent=2
        )
    return tabulator_data_output_path


def lookup_play(play_index):
    if not isinstance(play_index, str):
        play_index = str(play_index)
    global play_id_2_play_name
    if play_id_2_play_name is None:
        plays_filepath = f"{JSON_FOLDER}/plays.json"
        with open(plays_filepath, "r") as plays_file:
            play_id_2_play_name = json.load(plays_file)
    return play_id_2_play_name[play_index]


def delete_rows_in_dump(json_file_path: str, test_2_fieldname: dict):
    """
    test_2_fieldname: dict, 
        key = fieldname,
        val = function that takes fieldname 
        and returns true if it qualifies the row to be deleted
    """
    print(f"deleting rows in {json_file_path}")
    # load data
    json_data = None
    with open(json_file_path, "r") as json_file_io:
        json_data = json.load(json_file_io)
    # apply changes
    keys = list(json_data.keys())
    for entity_id in keys:
        entity = json_data[entity_id]
        delete_row = False
        for fieldname, testfunction in test_2_fieldname.items():
            delete_row = testfunction(entity[fieldname])
            if delete_row is not True:
                break
        if delete_row:
            _ = json_data.pop(entity_id)
    # dump data
    with open(json_file_path, "w") as outfile:
        json.dump(json_data, outfile, indent=2)
    return json_file_path


def modify_fields_in_dump(json_file_path: str, fieldnames_to_manipulations: dict):
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
            current_field_value = entity[fieldname] if fieldname in entity else None
            new_field_value = manipulation_function(current_field_value)
            entity[fieldname] = new_field_value
        json_data[entity_id] = entity
    # dump data
    with open(json_file_path, "w") as outfile:
        json.dump(json_data, outfile, indent=2)
    return json_file_path


def get_play_title_for_mentions(mentions: list):
    new_mentions = []
    if len(mentions) != 0:
        for mention in mentions:
            new_mentions.append(
                [
                    lookup_play(mention["id"])["title"],
                    mention["value"]
                ]
            )
    return new_mentions


if __name__ == "__main__":
    os.makedirs(JSON_FOLDER, exist_ok=True)
    json_file_paths = br_client.dump_tables_as_json(
        BASEROW_DB_ID,
        folder_name=JSON_FOLDER, 
        indent=2
    )
    places_filepath = f"{JSON_FOLDER}/places.json"
    vienna_places_filepath = f"{JSON_FOLDER}/vienna_places.json"
    if os.path.isfile(places_filepath):
        fieldnames_to_manipulations = {
            "geonames" : get_normalized_uri,
            "mentioned_in" : get_play_title_for_mentions
        }
        modfied_file_path = modify_fields_in_dump(
            places_filepath,
            fieldnames_to_manipulations
        )
        create_tabulator_data(
            json_file_path=modfied_file_path,
            name_key="name",
            lng_key="long",
            lat_key="lat",
            geonames_url_key="geonames",
            internal_id_key="nestroy_id",
            mentions_key="mentioned_in",
            altnames_keys=[
                "alt_tokens",
                "legacy"
            ],
            total_occurences_keys="total_occurences"
        )
    if os.path.isfile(vienna_places_filepath):
        test_2_fieldname = {
            "geonames" : lambda x: not bool(x.strip())
        }
        fieldnames_to_manipulations = {
            "geonames" : get_normalized_uri,
            "mentioned_in" : get_play_title_for_mentions
        }
        modfied_file_path = modify_fields_in_dump(
            vienna_places_filepath,
            fieldnames_to_manipulations
        )
        modfied_file_path = delete_rows_in_dump(
            modfied_file_path,
            test_2_fieldname
        )
        create_tabulator_data(
            json_file_path=modfied_file_path,
            name_key="survey_id",
            lng_key="long",
            lat_key="lat",
            geonames_url_key="geonames",
            internal_id_key="nestroy_id",
            mentions_key="mentioned_in",
            altnames_keys=[
                "variants",
                "modern_name"
            ],
            total_occurences_keys="total_occurences"
        )
    for path in json_file_paths:
        print(path)
