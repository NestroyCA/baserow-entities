from copy import deepcopy
import os
import json
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri
from config import br_client, BASEROW_DB_ID, JSON_FOLDER

play_id_2_play_name = None
altname_keys = ["alt_tokens", "legacy"]
lemmas_filepath = f"{JSON_FOLDER}/lemma_context.json"
existing_lemmas = {}
with open(lemmas_filepath, "r") as lemmafile:
    existing_lemmas = json.load(lemmafile)


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
    """creates dict for row if name, lng, lat and mentions are True, else None"""
    if name and lng and lat and mentions:
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
    else:
        return None


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
            new_row = make_tabulator_data_entry(
                name=row[name_key],
                lng=row[lng_key],
                lat=row[lat_key],
                geonames_url=row[geonames_url_key],
                internal_id=row[internal_id_key],
                mentions=row[mentions_key],
                alt_names=[row[altnames_key] for altnames_key in altnames_keys if row[altnames_key]],
                total_occurences=row[total_occurences_keys] if row[total_occurences_keys] else 1
            )
            if new_row is not None:
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


def modify_fields_in_dump(
        json_file_path: str,
        fieldnames_to_manipulations: dict,
        write:bool=True,
        index_name_key=""
    ):
    """
    Loads json file from json_file_path and performs a set of manipulations.
    fieldnames_to_manipulations contains a set of fieldnames-strings as keys, 
    defining the set of fields to be manipulated, and as corresponding value
    a function performing the desired manipulations
    """
    print(f"updating {', '.join(fieldnames_to_manipulations.keys())} in {json_file_path}")
    # load data
    json_data = None
    print(f"loading {json_file_path}")
    with open(json_file_path, "r") as json_file_io:
        json_data = json.load(json_file_io)
    # apply changes
    for entity_id in json_data.keys():
        entity = json_data[entity_id]
        entity["occurs_in_xml"] = ""
        if "nestroy_id" in entity:
            nestroy_id = entity["nestroy_id"]
            if nestroy_id in existing_lemmas:
                current_value = existing_lemmas[nestroy_id]
                if isinstance(current_value, list):
                    new_value = {
                        "matches" : current_value,
                        "index_name" : entity[index_name_key]
                    }
                    entity["occurs_in_xml"] = nestroy_id
                    existing_lemmas[nestroy_id] = new_value
        else:
            input(entity)
        for fieldname in fieldnames_to_manipulations:
            manipulation_function = fieldnames_to_manipulations[fieldname]
            current_field_value = entity[fieldname] if fieldname in entity else None
            new_field_value = manipulation_function(current_field_value)
            entity[fieldname] = new_field_value
        json_data[entity_id] = entity
    if write:
        # dump data
        with open(json_file_path, "w") as outfile:
            json.dump(json_data, outfile, indent=2)
        return json_file_path
    else:
        return json_data


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

def summarize_lemma_authority_data(json_data, authority_fieldnames):
    terms = []
    for term_entry in json_data.values():
        authorty_links = []
        for field_key, linklable in authority_fieldnames.items():
            link = term_entry.pop(field_key)
            if link:
                authorty_links.append(
                    (
                        linklable, link
                    )
                )
        term_entry["authority_data"] = authorty_links
        terms.append(term_entry)
    return terms

def unpack_domains(domains_list):
    if domains_list:
        return "\n".join([d["value"] for d in domains_list])
    return ""

def load_lemmas_in_context():
    with open("./json_dumps/lemma_context.json", "r") as lemmafile:
        return json.load(lemmafile)


if __name__ == "__main__":
    os.makedirs(JSON_FOLDER, exist_ok=True)
    json_file_paths = br_client.dump_tables_as_json(
        BASEROW_DB_ID,
        folder_name=JSON_FOLDER, 
        indent=2
    )
    places_filepath = f"{JSON_FOLDER}/places.json"
    vienna_places_filepath = f"{JSON_FOLDER}/vienna_places.json"
    person_filepath = f"{JSON_FOLDER}/persons.json"
    terms_filepath = f"{JSON_FOLDER}/terminology.json"
    if os.path.isfile(terms_filepath):
        authority_fieldnames = {
            "wikidata_url" : "Wikidata",
            "DWB_url" : "DWB",
            "other_lexical_url_a" : "other",
            "other_lexical_url_b" : "other",
        }
        fieldnames_to_manipulations = {
            "domains" : unpack_domains
        }
        modfied_data = modify_fields_in_dump(
            json_file_path=terms_filepath,
            fieldnames_to_manipulations=fieldnames_to_manipulations,
            write=False,
            index_name_key="lemma"
        )
        with open(terms_filepath, "w") as outfile:
            json.dump(
                summarize_lemma_authority_data(
                    modfied_data, authority_fieldnames
                ),
                outfile,
                indent=2
            )
    if os.path.isfile(person_filepath):
        fieldnames_to_manipulations = {
            "occurences" : get_play_title_for_mentions
        }
        modfied_data = modify_fields_in_dump(
            json_file_path=person_filepath,
            fieldnames_to_manipulations=fieldnames_to_manipulations,
            write=False,
            index_name_key="name"
        )
        with open(person_filepath, "w") as outfile:
            json.dump(
                [val for val in modfied_data.values()],
                outfile,
                indent=2
            )
    
    if os.path.isfile(places_filepath):
        fieldnames_to_manipulations = {
            "geonames" : get_normalized_uri,
            "mentioned_in" : get_play_title_for_mentions
        }
        modfied_file_path = modify_fields_in_dump(
            json_file_path=places_filepath,
            fieldnames_to_manipulations=fieldnames_to_manipulations,
            write=True,
            index_name_key="name"
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
            json_file_path=vienna_places_filepath,
            fieldnames_to_manipulations=fieldnames_to_manipulations,
            write=True,
            index_name_key="survey_id"        
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

    with open(lemmas_filepath, "w") as lemmafile:
        json.dump(existing_lemmas, lemmafile, indent=2)
        json_file_paths.append(lemmas_filepath)

    for path in json_file_paths:
        print(path)
