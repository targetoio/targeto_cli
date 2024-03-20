import sys
import boto3
import snowflake.connector
import click
import pandas as pd
import io
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

class ETLProcess:
    def __init__(self, local_column_list=None, matchable_ids_column_list=None, config_data=None):
        self.local_column_list = local_column_list
        self.matchable_ids_column_list = matchable_ids_column_list
        self.config_data = config_data


    # validate csv file
    def validate_client_data(self,client_data):
        # local_column_set, matchable_ids_column_list = extract_column_lists(config_data)
        try:
            df = client_data
            df.rename(columns=lambda x: x.lower(), inplace=True)
            df = df.replace('-', pd.NA).fillna('')

            # It ensures that at least one of the matchable_id columns specified in the configuration is present in the CSV file.
            if not any(col in df.columns for col in self.matchable_ids_column_list):
                click.echo(f"No columns with matchable IDs found in your data.")
                exit()

            # Verify that the columns defined in the configuration (config_columns) are present in the DataFrame (df).
            missing_columns = [col for col in self.local_column_list if col not in df.columns]
            if missing_columns:
                click.echo(f"The following columns are missing in your data: {missing_columns}")
                user_input = click.prompt("Do you want to continue? (yes/no)", type=str, default="").lower()

                if user_input != 'yes':
                    sys.exit()
        except pd.errors.ParserError as e:
            print(f"Error reading the CSV file: {e}")
        return df

    # connect with AWS S3 Bucket
    def connect_to_AWS_s3(self):
        try:
            aws_access_key_id = click.prompt('Enter aws_access_key_id', type=str)
            aws_secret_access_key = click.prompt('Enter aws_secret_access_key', type=str)
            bucket_name = click.prompt('Enter bucket_name', type=str)
            file_key = click.prompt('Enter file_key', type=str)

            # Create an S3 client
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

            # Fetch the CSV file content from S3
            s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
            data = s3_object['Body'].read().decode('utf-8')

            client_data = pd.read_csv(io.StringIO(data), usecols=lambda x: x.lower() in self.local_column_list)

            client_dataframe = self.validate_client_data(client_data)
        
            return client_dataframe
        except Exception as e:
            click.echo("Check your AWS credentials and permissions.")
            sys.exit()

    # connect to snowflake  
    def connect_to_snowflake(self):
        user = click.prompt('Enter your Snowflake username', type=str)
        password = click.prompt('Enter your Snowflake password', type=str)
        account = click.prompt('Enter your Snowflake account',type=str)
        warehouse = click.prompt('Enter your  Snowflake warehouse',type=str)
        database = click.prompt('Enter your Snowflake database',type=str)
        schema = click.prompt('Enter your Snowflake schema',type=str)
        table = click.prompt('Enter your Snowflake table',type=str)
        try:
            snowflake_conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                table=table
            )

            cur = snowflake_conn.cursor()

            # Check if all columns exist in the Snowflake table
            existing_columns_query = "SHOW COLUMNS IN TABLE CALL_CENTER"
            cur.execute(existing_columns_query)

            existing_columns = [row[2] for row in cur.fetchall()]
            existing_columns_lower = [col.lower() for col in existing_columns]

            selected_columns = [col for col in self.local_column_list if col in existing_columns_lower]

            if selected_columns:
                # Execute the SELECT query with the desired columns
                query = 'SELECT {} FROM CALL_CENTER'.format(','.join(selected_columns))
                cur.execute(query)

                results = cur.fetchall()

                # Create Pandas DataFrame with selected columns
                client_data = pd.DataFrame(results, columns=selected_columns)

                client_dataframe = self.validate_client_data(client_data)

                cur.close()
                snowflake_conn.close()

                return client_dataframe
            else:
                click.echo("None of the columns in the configuration exist in the Snowflake table.")
                sys.exit()

        except Exception as e:
            click.echo("Check your Snowflake credentials and permissions.")
            sys.exit()


    # connect_to_firebase():
    def connect_to_firebase(self):
        try:
            key_file_path = click.prompt('Enter the path to your Firebase service account key JSON file:', type=str)
            database_URL = click.prompt('Enter database URL', type=str)
            node_path = click.prompt('Enter the path to the Firebase node', type=str)

            cred = credentials.Certificate(key_file_path)
            firebase_admin.initialize_app(cred, {
                'databaseURL': database_URL
            })

            # Reference to Firebase database
            ref = db.reference(node_path)

            # Retrieve data from Firebase
            data = ref.get()

            # Convert the filtered data to a pandas DataFrame
            client_data = self.get_client_data_from_firebase(data)

            client_dataframe = self.validate_client_data(client_data)

            # Clean up resources (optional)
            firebase_admin.delete_app(firebase_admin.get_app())

            return client_dataframe
        except Exception as e:
            click.echo("Check your FIREBASE credentials and permissions.")
            sys.exit()

    def get_client_data_from_firebase(self, firebase_data):
        client_data_list = []

        def iterate_object(data):
            for key, value in data.items():
                if not isinstance(value, dict):
                    if data not in client_data_list:
                        client_data_list.append(data)
                else:
                    iterate_object(value)

        iterate_object(firebase_data)

        client_data_based_on_config = []
        for entry in client_data_list:
            filtered_entry = {}
            for key in self.local_column_list:
                if key in entry:
                    filtered_entry[key] = entry[key]
            client_data_based_on_config.append(filtered_entry)

        # Convert the filtered data to a pandas DataFrame
        client_data = pd.DataFrame(client_data_based_on_config)
        return client_data
