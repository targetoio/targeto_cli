import os
import sys
import pandas as pd
import yaml
from appdirs import user_data_dir

app_data_dir = user_data_dir(appname='targeto', appauthor='Abhishek')

# import collection list from the yml file
def import_collection_lists(file_path):
    with open(file_path, "r") as yaml_file:
        yml_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
        collections = yml_data.get('collections', [])
    
    collection_list = [
        {key.strip(): value.strip() if isinstance(value, str) else value for key, value in item.items()}
        for item in collections
        if all(
            (key.strip() == 'collection_key' and (isinstance(value, str)) and (value is not None) and len(value) == 43 and value.strip() not in ['', '-', 'nan']) or
            (key.strip() == 'collection_id' and (isinstance(value, str)) and value.strip() not in ['', None, '-', 'nan'])
            for key, value in item.items()
        )
    ]

    return collection_list

# get collection list from the yml file
def get_collection_list():
    file_path = os.path.join(app_data_dir, 'collections_list.yml')

    try:
        with open(file_path, "r") as yaml_file:
            collection_list = yaml.load(yaml_file, Loader=yaml.FullLoader)
            if collection_list is None:
                collection_list = {} 
                collection_list["collections"] = []
            return collection_list
    except FileNotFoundError:
        collection_list = {"collections": []}
        # Save the empty collection list to the file
        with open(file_path, "w") as new_yaml_file:
            yaml.dump(collection_list, new_yaml_file, default_flow_style=False)
        return collection_list
    except Exception as e:
        sys.exit()
    
# save collection list to the yml file
def save_collection_list(collection_list):
    file_path = os.path.join(app_data_dir, 'collections_list.yml')

    with open(file_path, "w") as yaml_file:
        yaml.dump(collection_list, yaml_file, default_flow_style=False)

