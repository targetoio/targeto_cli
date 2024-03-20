# # import firebase_admin
# # from firebase_admin import credentials
# # from firebase_admin import db
# # import pandas as pd

# # def connect_to_firebase():
# #     cred = credentials.Certificate("/Users/suman/Downloads/number2nd-firebase-adminsdk-nsk8w-d0c595f54c.json")
# #     firebase_admin.initialize_app(cred, {
# #         'databaseURL': 'https://number2nd-default-rtdb.europe-west1.firebasedatabase.app/'
# #     })

# #     # Reference to Firebase database
# #     ref = db.reference('/user_contacts/')

# #     data = ref.get()

# #     result_list = []

# #     def iterate_dict(d):
# #         for key, value in d.items():
# #             if not isinstance(value, dict):
# #                 if d not in result_list:
# #                     result_list.append(d)
# #             else:
# #                 iterate_dict(value)

# #     iterate_dict(data)
# #     # return result_list

# #     local_column_list = ['name', 'phoneNumber']

# #     client_data_based_on_config = []
# #     for entry in result_list:
# #         filtered_entry = {}
# #         for key in local_column_list:
# #             if key in entry:
# #                 filtered_entry[key] = entry[key]
# #         client_data_based_on_config.append(filtered_entry)

# #     # Convert the filtered data to a pandas DataFrame
# #     client_data = pd.DataFrame(client_data_based_on_config)
# #     return client_data

# # print(connect_to_firebase())

import secrets
import string

def generate_random_string(length):
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

random_string = generate_random_string(43)
print(random_string)
print(len('blNLadd2LjPPDpvo0mTrvVQBBaKr8FE7AlY-DLVukDM'))

