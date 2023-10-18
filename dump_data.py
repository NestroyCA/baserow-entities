from copy import deepcopy
import os
import json
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri
from config import br_client, BASEROW_DB_ID, JSON_FOLDER

play_id_2_play_name = None
altname_keys = ["alt_tokens", "legacy"]

def get_link(title, target, field="", classes=""):
    # return {
    #     "title":title,
    #     "field": field,
    #     "formatter":"link", 
    #     "formatterParams":{
    #         "labelField":"name",
    #         "urlPrefix":"",
    #         "target": target,
    #     }
    # }
    return f"<a class='{classes}' href='{target}'>{title}</a>"

def get_list(list_content):
    return f"<ul>{''.join([f'<li>{c}</li>' for c in list_content])}</ul>"

def create_tabulator_data(features):
    tabulator_data_output_path = f"{JSON_FOLDER}/tabulator_data.json"
    tabulator_data = []
    for feature in features:
        row = {
            # "coordinates" : feature.pop("geometry").pop("coordinates")
        }
        alt_names = ""
        mentions = ""
        for key, val in feature.pop("properties").items():
            if key in altname_keys:
                if val:
                    if alt_names == "":
                        alt_names = val
                    else:
                        alt_names += f", {val}"
                row["alt_names"] = alt_names
            elif key == "mentioned_in":
                links = []
                for mention in val:
                    play_nestroy_id = mention["value"]
                    play_title = mention["title"]
                    link = f"{get_link(play_title, play_nestroy_id)}"
                    links.append(link)
                mentions = get_list(links)
                row["mentions"] = mentions
            elif key == "geonames":
                target = val
                title = key
                field = key
                row[key] = get_link(title, target, field)
            elif key == "name":
                coordinates = feature.pop("geometry").pop("coordinates")
                row[key] = get_link(
                    title=val,
                    target=f"#{coordinates[0]}/{coordinates[1]}",
                    classes="leaflet_mappoint_link"
                )
            else:
                row[key] = val
        tabulator_data.append(row)
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
            current_field_value = entity[fieldname] if fieldname in entity else None
            new_field_value = manipulation_function(current_field_value)
            entity[fieldname] = new_field_value
        json_data[entity_id] = entity
    # dump data
    with open(json_file_path, "w") as outfile:
        json.dump(json_data, outfile, indent=2)


def make_geoname_point(long_lat: tuple, properties: dict):
    if len(properties["mentioned_in"]) == 0:
        return None
    mentions = properties["mentioned_in"]
    properties["mentioned_in"] = []
    for mention in mentions:
        play_index = mention["id"]
        play_data = lookup_play(play_index)
        mention["title"] = play_data["title"]
        properties["mentioned_in"].append(mention)
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": list(long_lat)
        },
        "properties": properties
    }


def create_geo_json(json_dump_filepath:str=None, json_dump_input:json=None):
    print(f"creating geojson from {json_dump_filepath}")
    features = []
    json_dump = None
    if json_dump_filepath is not None:
        with open(json_dump_filepath, "r") as json_dump_file:
            json_dump = json.load(json_dump_file)
    elif json_dump_input is not None:
        json_dump = json_dump_input
    else:
        print("no valid arg for accessing json-data")
        raise ValueError
    for json_entity in json_dump.values():
        lat = json_entity.pop("lat")
        long = json_entity.pop("long")
        if lat and long:
            # # remove useless data
            _ = json_entity.pop("id")
            _ = json_entity.pop("order")
            long_lat = (long, lat)
            geoname_point = make_geoname_point(
                long_lat = long_lat,
                properties=json_entity
            )
            if geoname_point is not None:
                features.append(geoname_point)
    
    dump_data = {
        "type": "FeatureCollection",
        "features": features
    }
    print(f"prepared data for tabulator in {create_tabulator_data(deepcopy(features))}")
    new_filepath = json_dump_filepath.replace(".json", "_geodata.json")
    with open(new_filepath, "w") as geo_data_dumpfile:
        json.dump(
            dump_data,
            fp=geo_data_dumpfile,
            indent=2
        )
    return new_filepath


if __name__ == "__main__":
    os.makedirs(JSON_FOLDER, exist_ok=True)
    json_file_paths = br_client.dump_tables_as_json(
        BASEROW_DB_ID,
        folder_name=JSON_FOLDER, 
        indent=2
    )
    places_filepath = f"{JSON_FOLDER}/places.json"
    if os.path.isfile(places_filepath):
        fieldnames_to_manipulations = {
            "geonames" : get_normalized_uri
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
    geo_json_filepath = create_geo_json(places_filepath)
    print(f"wrote geojson to {geo_json_filepath}")
