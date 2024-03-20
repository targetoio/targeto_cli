from itertools import zip_longest
import re
import sys
import click
import pandas as pd
import time
import phonenumbers
import secrets
import string
from collections import defaultdict
from .normalization import value_mapping_for_integer, value_mapping_with_standard_values, value_mapping_of_string
from .anonymization import get_blinded_value


# get local column list and matchable_ids column list
class CreateProfile:
    def __init__(self,config_data=None):
        self.config_data = config_data
        self.invalid_email = []
        self.invalid_numbers = []

    # get profile_structure
    def get_profile_structure(self,client_dataframe):        
        grouped_config = defaultdict(list)

        profile_structure = {}
        for column in self.config_data['config']:
            if column['local_column_name'].lower() in client_dataframe:
                client_dataframe.rename(columns={column['local_column_name'].lower(): column['bucket_column_name']}, inplace=True)
                column_type_value = column['column_type']['column_type'] if 'column_type' in column['column_type'] else ''
                profile_structure.setdefault(column['column_type']['column_parent_type'], {}).update({column['bucket_column_name']: column_type_value})
                grouped_config[column['column_type']['column_parent_type']].append(column)

        # divide the config data into 3 different list of matchable_ids, custom_ids and signals list
        config_matchable_ids = grouped_config["matchable_ids"]
        config_custom_ids = grouped_config["custom_ids"]
        config_signals = grouped_config["signals"]

        return client_dataframe,profile_structure, config_matchable_ids, config_custom_ids, config_signals

    # verify phone number and convert into e.164 formate
    def validate_phone_number(self,phone_number):
        try:
            parsed_number = phonenumbers.parse(phone_number, "US")
            if phonenumbers.is_valid_number(parsed_number):
                e164_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                return e164_number
            else:
                self.invalid_numbers.append(phone_number)
                return None
        except Exception as e:
            self.invalid_numbers.append(phone_number)
            return None

    # verify email address
    def validate_email(self,email):
        try:
            pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
            if re.match(pattern, email):
                return email
            else:
                self.invalid_email.append(email)
                return None    
        except Exception as e:
            self.invalid_email.append(email)
            return None
            

    # anonymization of matchable ids 
    def anonymization_of_matchable_ids(self,df,config_matchable_ids):
        for column in config_matchable_ids:
            column_type = column['column_type']['column_type']
            bucket_column_name = column['bucket_column_name']
            
            if column_type == 'email':
                df[bucket_column_name] = df[bucket_column_name].map(lambda x: self.validate_email(x))
            elif column_type == 'phone_number':
                df[bucket_column_name] = df[bucket_column_name].map(lambda x: self.validate_phone_number(x))
        
        return df

    # normalization of signals
    def normalize_signals(self,df,config_signals):
        standard_values = self.config_data['standard_values']

        for column in config_signals:
            column_type = column['column_type']['column_type']
            bypass_values = column['column_type']['bypass_values']
            bucket_column_name = column['bucket_column_name']
            age_range_dict = next((item for item in standard_values if item["column_type"] == column_type), None)
            values_data = age_range_dict['values_data']
            data_type = age_range_dict['data_type']

            if data_type == 'string':
                if "value_mapping" in column:
                    value_mapping = column['value_mapping']
                    df[bucket_column_name] = df[bucket_column_name].map(lambda x: value_mapping_with_standard_values(x, value_mapping)).map(lambda x : value_mapping_of_string(x,values_data,bypass_values))
                else:
                    df[bucket_column_name] = df[bucket_column_name].map(lambda x : value_mapping_of_string(x,values_data,bypass_values))
            elif data_type == 'integer':
                if "value_mapping" in column:
                    value_mapping = column['value_mapping']
                    df[bucket_column_name] = df[bucket_column_name].map(lambda x: value_mapping_with_standard_values(x, value_mapping)).map(lambda x: value_mapping_for_integer(x, bypass_values, values_data))
                else:
                    df[bucket_column_name] = df[bucket_column_name].map(lambda x: value_mapping_for_integer(x, bypass_values, values_data))
        return df
        
    # generate profile list from the user data
    def generate_profile_list(self,client_dataframe, collection_key):
        df, profile_structure,config_matchable_ids, config_custom_ids, config_signals = self.get_profile_structure(client_dataframe)
        profiles = {}
        
        # anonymization of matchable ids
        self.anonymization_of_matchable_ids(df,config_matchable_ids)

        # normalization of signals
        self.normalize_signals(df, config_signals)

        # get matchable_ids, custom_ids and signals from the user data
        file_matchable_ids = df[profile_structure.get('matchable_ids', {}).keys()].replace('-', pd.NA).to_dict(orient='records')
        file_custom_ids = df[profile_structure.get('custom_ids',{}).keys()].replace('-', pd.NA).to_dict(orient='records')
        file_signals = df[profile_structure.get('signals', {}).keys()].replace('-', pd.NA).to_dict(orient='records')

        # convert matchable_ids, custom_ids and signals into profiles

        for signals, custom_ids, matchable_ids in zip_longest(file_signals, file_custom_ids, file_matchable_ids, fillvalue={}):
            signals_value = {key: value for key, value in signals.items() if pd.notna(value)}
            custom_ids_value = {key: value for key, value in custom_ids.items() if pd.notna(value)}
            matchable_ids_value = {key: (get_blinded_value(collection_key, value))[:43] for key, value in matchable_ids.items() if pd.notna(value)}

            if matchable_ids_value:
                key_for_match_list, match_key = next(iter(matchable_ids_value.items()))
                if match_key in profiles:
                    random_string = string.ascii_letters + string.digits
                    match_key =  ''.join(secrets.choice(random_string) for _ in range(43))
                row_dict = {
                    'matchable_ids': matchable_ids_value,
                    'custom_ids': custom_ids_value,
                    'signals': signals_value,
                }
                profiles[match_key] = row_dict
            else:
                continue
                
        click.echo(f"Invalid Email: {len(self.invalid_email)}")
        click.echo(f"Invalid Phone Numbers: {len(self.invalid_numbers)}")
        
        if profiles:
            return profiles, profile_structure
        else:
            click.echo("No profiles found in your data.")
            sys.exit()

    
