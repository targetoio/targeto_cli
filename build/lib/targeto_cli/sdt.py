import base64
import sys
import click
import requests
from queue import Queue

from .utils import get_collection_list, save_collection_list
from .api import GET_TRANSFORMATION_STRUCTURE_API, GET_SDT_DATA_API, SET_SDT_DATA_API, VALIDATION_API
from .anonymization import get_blinded_value

class SDT():
    def __init__(self, transformation_token=None):
        self.transformation_token = transformation_token
        self.collection_key = None
        

    def get_transformation_structure(self):
        """
        Get the transformation structure from the API.
        """
        base_url = GET_TRANSFORMATION_STRUCTURE_API + self.transformation_token
        try:
            response = requests.get(base_url)

            if response.json()['success'] == True:
                transformation_structure = response.json()['data']
                return transformation_structure
            else:
                click.echo(response.json()['message'])
                sys.exit()
        except Exception as e:
            sys.exit()

    def validate_collection_with_token(self, collection_id, collection_key):
        # print(collection_id)
        try:
            base64_collection_key = base64.urlsafe_b64decode((collection_key + "=").encode('utf-8'))
            validation_key = get_blinded_value(base64_collection_key, collection_id)
            print(validation_key)
            validation_data = {'collection_id': collection_id, 'key': validation_key}
        except Exception as e:
            click.echo("Please Enter valid collection_key.")
            sys.exit()
        try:
            response = requests.post(VALIDATION_API, json=validation_data)
            if response.json()['success'] == True:     
                print(response.json()['message'])   
                return base64_collection_key
            else:
                print(response.json()['message'])   

                sys.exit()

        except Exception as e:
            sys.exit()

        
    def validate_collection_id(self):
        collection_id = self.get_transformation_structure()['collection_id']

        collection_list = get_collection_list()

        for collection in collection_list["collections"]:
            if collection["collection_id"] == collection_id:
                if "collection_key" in collection:
                    base64_collection_key = self.validate_collection_with_token(collection_id, collection['collection_key'])
                    return base64_collection_key
                else:
                    collection_key = click.prompt('Enter collection_key', type=str)
                    base64_collection_key = self.validate_collection_with_token(collection_id, collection_key)

                    collection["collection_key"] = collection_key
                    save_collection_list(collection_list)
                    return base64_collection_key
        else:
            collection_key = click.prompt('Enter collection_key', type=str)
            base64_collection_key = self.validate_collection_with_token(collection_id, collection_key)

            collection_list["collections"].append({"collection_id": collection_id, "collection_key": collection_key})

            save_collection_list(collection_list)
            return base64_collection_key
                        

    def get_sdt_data(self, sdt_id):
        """
        Get the SDT data for a given ID.
        """
        base_url = GET_SDT_DATA_API + sdt_id

        params = {
            'token': self.transformation_token
        }
        try:
            response = requests.get(base_url, params=params)
            if response.json()['success']:
                sdt_data = response.json()['data']['transferd_data']
                return sdt_data
        except Exception as e:
            return 

    def set_sdt_data(self, sdt_id, transaction_status, transformed_data):
        """
        Set the SDT data for a given ID.
        """
        base_url = SET_SDT_DATA_API + sdt_id
        
        payload = {
            'token': self.transformation_token,
            'transaction_status': transaction_status,
            'transfored_data': transformed_data
        }

        try:
            response = requests.put(base_url, json=payload)
        except Exception as e:
            return

    def multy_blinding_process(self, sdt_data):
        """
        Perform multy blinding process on the SDT data.
        """
        try:
            # collection_key = self.validate_collection_id()
            def blinded_value(first_blinded_value):
                return [get_blinded_value(self.collection_key,v) for v in first_blinded_value]

            multy_blinded_sdt_data = {key: blinded_value(value) for key, value in sdt_data.items()}
            return multy_blinded_sdt_data
        except Exception as e:
            return None
        
    def transfer_data_for_partner(self, sdt_id):
        sdt_data = self.get_sdt_data(sdt_id)
        if sdt_data:
            multy_blinded_sdt_data = self.multy_blinding_process(sdt_data)
            if multy_blinded_sdt_data:
                self.set_sdt_data(sdt_id, 'COMMIT', multy_blinded_sdt_data)
            else:
                self.set_sdt_data(sdt_id, 'ROLLBACK', {})
       
    def sdt_process(self):
        """
        Perform the SDT process.

        """
        self.collection_key = self.validate_collection_id()
        
        while True:
            transformation_structure = self.get_transformation_structure()
            free_partner = None
            max_size = 0
            other_data = transformation_structure['other_data']
            if other_data:
                for key, value in other_data.items():
                    if value["status"] == "FREE" and value["size_in_mb"] >= max_size:
                        free_partner = key
                        max_size = value["size_in_mb"]
                
                if free_partner:
                    self.transfer_data_for_partner(free_partner)
                else:
                    click.echo("Please try again later as some of your partners are currently locked for secure data transfer process.")
                    break
            else:
                click.echo("Your Data transformation process is completed.")
                break

           
    
    
