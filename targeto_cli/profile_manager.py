import base64
import os
import sys
import click
import pandas as pd
import requests
from .api import CREATE_COLLECTION_API, UPLOAD_PROFILE_API, VALIDATION_API, CONFIG_API
from .utils import get_collection_list, save_collection_list
from .create_profile import CreateProfile
from .anonymization import get_blinded_value
from .etl_process import ETLProcess

class ProfileManager:
    def __init__(self, config_id=None, collection_id=None, token=None):
        self.config_id = config_id
        self.collection_id = collection_id
        self.token = token
        self.upload_method = None

    # Create collection
    def create_collections(self, collection_name, description):
        if not self.validate_token({'token': self.token}):
            return
        
        if not collection_name:
            collection_name = click.prompt('Enter collection name', type=str)
        
        if not description:
            description = click.prompt('Enter description', type=str)
        
        payload = {"collection_name": collection_name, "description": description, "profile_type": 'NOT_MATCHABLE'}
        
        response = requests.post(CREATE_COLLECTION_API, json=payload, headers={"api-token": self.token})
        
        if response.json()['success'] == True:
            click.echo(response.json()['message'])
            collection_id = response.json().get('data', {}).get('collection_id')
            collection_name = response.json().get('data', {}).get('collection_name')
            
            collection_list = get_collection_list()
            collection_list["collections"].append({"collection_id": collection_id, "collection_name": collection_name})
            save_collection_list(collection_list)
        else:
            click.echo(response.json()['message'])

    # This method presents users with options to upload their data, allowing them to choose their preferred method for the data upload.
    def select_upload_method(self):
        options = ['AMAZON WEB SERVICES', 'SNOWFLAKE', 'FIREBASE', 'CSV_FILE']
        
        for i, option in enumerate(options, start=1):
            click.echo(f"{i}. {option}")
        
        while True:
            choice = input("Select your preferred method for uploading profile: ")
            if choice.isdigit() and 1 <= int(choice) <= len(options):
                return options[int(choice) - 1]
            else:
                click.echo("Invalid choice. Please try again.")

    # Get config data using config_id
    def get_config_data(self):
        if not self.config_id:
            self.config_id = click.prompt('Enter config id', type=str)

        params = {
        'include_values': 'true',
        'config_id': self.config_id
        }
        try:
            response = requests.get(CONFIG_API, params=params, headers={"api-token": self.token})

            if response.json()['success'] == True:        
                config_data = response.json()['data'] 
                return config_data
            else:
                print(response.json()['message'])
                sys.exit()
        except Exception as e:
            print(f"Error: {e}")

    # This method is for getting the local column list and matchable_ids column list from the config data
    def extract_column_lists(self,config_data):
        local_column_set = set()
        matchable_ids_column_list = []

        for column in config_data["config"]:
            local_column_set.add(column["local_column_name"])
            if column["column_type"]["column_parent_type"] == "matchable_ids":
                matchable_ids_column_list.append(column["local_column_name"])

        return local_column_set, matchable_ids_column_list


    # Get profiles from the user data based on the config data
    def get_profiles(self, collection_key):
        self.upload_method = self.select_upload_method()

        # Get config data using config_id
        config_data = self.get_config_data()
        local_column_set, matchable_ids_column_list = self.extract_column_lists(config_data)
        local_column_set_lower = [x.lower() for x in local_column_set]
        matchable_ids_column_list_lower = [x.lower() for x in matchable_ids_column_list]

        create_profile = CreateProfile(config_data=config_data)
        etl_process = ETLProcess(local_column_list=local_column_set_lower, matchable_ids_column_list=matchable_ids_column_list_lower, config_data=config_data)

        if self.upload_method == 'CSV_FILE': 
            file_path = click.prompt('Please provide the path to the file to upload', type=click.Path(exists=True))
            _, file_extension = os.path.splitext(file_path)
            file_format = file_extension[1:].lower()
            ALLOWED_FORMATS = ['xlsx', 'csv', 'tsv']

            if file_format in ALLOWED_FORMATS:
                # Read the file
                client_data = pd.read_csv(file_path, usecols=lambda x: x.lower() in local_column_set_lower)
                client_dataframe = etl_process.validate_client_data(client_data)
                # Generate profile list from the file
                profiles = create_profile.generate_profile_list(client_dataframe, collection_key)
                return profiles
            else:
                click.echo(f"File format '{file_format}' is not allowed.")
                sys.exit()

        elif self.upload_method == 'AMAZON WEB SERVICES':
            client_dataframe = etl_process.connect_to_AWS_s3()
            profiles = create_profile.generate_profile_list(client_dataframe, collection_key)
            return profiles

        elif self.upload_method == 'FIREBASE':
            client_dataframe = etl_process.connect_to_firebase()
            profiles = create_profile.generate_profile_list(client_dataframe, collection_key)
            return profiles
        
        elif self.upload_method == 'SNOWFLAKE':
            client_dataframe = etl_process.connect_to_snowflake()
            profiles = create_profile.generate_profile_list(client_dataframe, collection_key)
            return profiles
        

    # validate token for create collection and upload profiles
    def validate_token(self, data):
        try:
            response = requests.post(VALIDATION_API, json=data)
            
            if response.json()['success'] == True:
                return True
            else:
                click.echo(response.json()['message'])
                sys.exit()
        except requests.RequestException as e:
            click.echo(f"Exception is: {e}")
            sys.exit()

    # This method uploads the profiles to the Collection
    def upload_profiles(self,collection_key, validation_data):
        if not self.validate_token(validation_data):
            sys.exit()
        
        collection_key_str = base64.urlsafe_b64decode((collection_key + "=").encode('utf-8'))
        validation_key = get_blinded_value(collection_key_str, self.collection_id)
        
        profiles, profile_structure = self.get_profiles(collection_key_str)
        
       
        payload = {"profiles" : profiles, "profile_structure": profile_structure, "collection_id": self.collection_id, "profile_type": 'NOT_MATCHABLE', "key": (validation_key)[:43] }
        response = requests.post(UPLOAD_PROFILE_API, json=payload, headers={"api-token": self.token})

        if response.json()['success'] == True:
            click.echo(response.json()['message'])
        else:
            click.echo(f"response is: {response.json()['message']}")

    # This method validate the collection using token and collection_key
    def validate_collection_with_token(self):
        collection_list = get_collection_list()

        for collection in collection_list["collections"]:
            if not self.collection_id:
                self.collection_id = click.prompt('Enter collection_id', type=str)

            if collection["collection_id"] == self.collection_id:
                # Check if 'scalar' key is present

                if "collection_key" in collection:
                    try:
                        collection_key_str = collection['collection_key'] + "="
                        s = base64.urlsafe_b64decode(collection_key_str.encode('utf-8'))

                        validation_key = get_blinded_value(s, self.collection_id)
                        # print(validation_key)
                    except Exception as e:
                        click.echo("collection key is corrupted.")
                        sys.exit()

                    validation_data = {'token': self.token, 'collection_id': self.collection_id, 'key': (validation_key)[:43]}
                    self.upload_profiles(collection['collection_key'], validation_data)
                else:
                    try:
                        collection_key = click.prompt('Enter collection_key', type=str)
                        collection_key_str = collection_key + "="
                        s = base64.urlsafe_b64decode(collection_key_str.encode('utf-8'))

                        validation_key = get_blinded_value(s, self.collection_id)
                    except Exception as e:
                        click.echo("Please enter a valid collection key.")
                        sys.exit()

                    collection["collection_key"] = collection_key

                    validation_data = {'token': self.token, 'collection_id': self.collection_id, 'key': validation_key}

                    self.upload_profiles(collection['collection_key'], validation_data)
                    save_collection_list(collection_list)
                return 
        else:
            try:
                collection_key = click.prompt('Enter collection_key', type=str)
                collection_key_str = collection_key + "="
                s = base64.urlsafe_b64decode(collection_key_str.encode('utf-8'))

                validation_key = get_blinded_value(s, self.collection_id)
            except Exception as e:
                click.echo("Please enter a valid collection key.")
                sys.exit()

            collection_list["collections"].append({"collection_id": self.collection_id, "collection_key": collection_key})
            validation_data = {'token': self.token, 'collection_id': self.collection_id, 'key': validation_key}

            self.upload_profiles(collection_key, validation_data)
            save_collection_list(collection_list)
            return
        
        
