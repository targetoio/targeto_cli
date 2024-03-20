import click
from datetime import datetime
import os
import pandas as pd
import yaml

from .sdt import SDT
from .api import CREATE_COLLECTION_API, UPLOAD_PROFILE_API, VALIDATION_API
from .utils import import_collection_lists, get_collection_list, save_collection_list
from .profile_manager import ProfileManager

@click.group()
@click.version_option(package_name='targeto')
def cli():
    pass

    
# collection list command
@click.command()
def collection_list():
    try:
        collection_list = get_collection_list()
        if collection_list.get('collections'):
            click.echo(f"{'collection_id':<30} {'collection_key'}")
            click.echo(f"")
            for collection in collection_list["collections"]:
                collection_id = collection.get('collection_id', '-')
                collection_key = collection.get('collection_key', '-')
                
                click.echo(f"{collection_id:<30} {collection_key}")
        else:
            click.echo("Collection does not exists.") 
    except Exception as e:
        click.echo("Collection does not exists.")


# Command for uploading a profile
@click.command()
@click.argument('config_id', type=str, required=False)
@click.argument('collection_id', type=str, required=False)
# @click.argument('file_path', type=click.Path(exists=True), required=False)
def upload_profile(config_id, collection_id):
    try:    
        token = click.prompt('Enter your token', type=str)
        upload = ProfileManager(config_id=config_id, collection_id=collection_id, token=token)
        upload.validate_collection_with_token()
    except FileNotFoundError as e:
        click.echo("Collection does not exist.")


# export into a file command
@click.command()
@click.argument('file_path', type=click.Path(exists=True), required=False)
def export(file_path):
    try:
        if not file_path:
            file_path = click.prompt('Please provide the path to the file to upload', type=click.Path(exists=True))

        collections = get_collection_list()
        # df = pd.DataFrame(collections)
        current_date = datetime.now().strftime('%Y%m%d')

        counter = 0
        file_name = f'collection-list_{current_date}.yml'

        yml_file_path = os.path.join(file_path, file_name)
        while os.path.exists(yml_file_path):
            counter += 1
            file_name = f'collection-list_{current_date}({counter}).yml'
            yml_file_path = os.path.join(file_path, file_name)
        with open(yml_file_path, 'w') as yml_file:
            yaml.dump(collections, yml_file, default_flow_style=False)
    except FileNotFoundError as e:
        click.echo("Collection does not exists.")
        
# import collection list command
@click.command()
@click.argument('file_path', type=click.Path(exists=True), required=False)
def import_collection_list(file_path):
    if not file_path:
        file_path = click.prompt('Please provide the path to the file to upload', type=click.Path(exists=True))

    _, file_extension = os.path.splitext(file_path)
   
    if file_extension.lower() == '.yml':
        try:
            existing_collections = get_collection_list()
            existing_collections['collections'].extend(import_collection_lists(file_path))
            save_collection_list(existing_collections)
        except FileNotFoundError as e:
            click.echo("Collection does not exists.")
    else:
        click.echo("Please provide csv file.")

# data transformation command(for multi blinding process)
@click.command()
@click.argument('api_token', type=str, required=False)
@click.argument('transformation_token', type=str, required=False)
def data_transformation(api_token, transformation_token):
    if not api_token:
        api_token = click.prompt('Enter api token', type=str)
    
    if not transformation_token:
        transformation_token = click.prompt('Enter transformation token', type=str)

    transformation = SDT(api_token=api_token, transformation_token=transformation_token)
    # transformation.validate_collection_id()
    transformation.sdt_process()

cli.add_command(upload_profile)
cli.add_command(collection_list)
cli.add_command(export)
cli.add_command(import_collection_list)
cli.add_command(data_transformation)

if __name__ == '__main__':
    cli()

