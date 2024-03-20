import re
import pandas as pd

def value_mapping_with_standard_values(value, value_mapping):
    """
    Maps value to a standard value based on a given set of conditions.

    Args:
        value (str): The input value to be mapped.
        value_mapping (list): List of dictionaries containing conditions and their corresponding standard values.

    Returns:
        str: The mapped standard value, or the original value if no mapping is found.
    """
    try:
        for condition in value_mapping:
            if condition["is_int_mapping"]:
                # If the condition is for integer mapping
                try:
                    int_value = int(value)
                except ValueError:
                    continue
                if 'min' in condition and 'max' in condition:
                    if condition["min"] is not None and condition["max"] is not None:
                        # If the condition has both min and max range, check if the input value falls within the range
                        if condition["min"] <= int_value <= condition["max"]:
                            return condition["standard_value"]
                    elif condition["max"] is not None:
                        # If the condition has only max range, check if the input value is less than or equal to the max range
                        if int_value <= condition["max"]:
                            return condition["standard_value"]
                    elif condition["min"] is not None:
                        # If the condition has only min range, check if the input value is greater than or equal to the min range
                        if int_value >= condition["min"]:
                            return condition["standard_value"]
                elif "max" in condition:
                    if condition["max"] is not None:
                        # If the condition has only max range, check if the input value is less than or equal to the max range
                        if int_value <= condition["max"]:
                            return condition["standard_value"]
                elif "min" in condition:
                    if condition["min"] is not None:
                        # If the condition has only min range, check if the input value is greater than or equal to the min range
                        if int_value >= condition["min"]:
                            return condition["standard_value"]
            else:
                # If the condition is for string mapping
                mapping_value = value.lower()
                include = [item.lower() for item in condition['include']] if 'include' in condition else None
                exclude = [item.lower() for item in condition['exclude']] if 'exclude' in condition else None
                if include and exclude:
                    # If both include and exclude lists are provided, check if the value is present in both lists
                    if mapping_value in include and mapping_value in exclude:
                        return condition["standard_value"]
                elif include is not None:
                    # If only include list is provided, check if the value is present in the include list
                    if mapping_value in include:
                        return condition["standard_value"]
                elif exclude is not None:
                    # If only exclude list is provided, check if the value is not present in the exclude list
                    if mapping_value not in exclude:
                        return condition["standard_value"]
        return value

    except Exception as e:
        return value

# this function is used to get the digits from a string, when the value mapping is not found for the integer data_type columns
def get_digits(string):
    digits = re.findall(r'\d+', string)
    return digits[0] if digits else None

def value_mapping_for_integer(value, bypass, values_data):
    """
    Maps an integer value to a standard value based on a given range.

    Args:
        value (int): The input value to be mapped.
        bypass (bool): Flag indicating whether to bypass the mapping and return the original value.
        values_data (list): List of dictionaries containing standard values and their corresponding ranges.

    Returns:
        dict: A dictionary containing the unique ID and parent ID of the mapped standard value,
              or the original value if bypass is True and no mapping is found.
    """
    try:
        if not value:
            return None
        
        elif value:
            direct_match = next((sv for sv in values_data if sv['value'] == value), None)
            if direct_match:
                return {'unique_id': direct_match['unique_id'], 'parent_id': direct_match['parent_id']}
            
            # If the input value is not empty, check for a matching range in the standard values
            for standard_value in values_data:
                try:
                    int_value = int(value)
                except ValueError:
                    continue
                if standard_value['min'] is not None and standard_value['max'] is not None:
                    # If the standard value has both min and max range, check if the input value falls within the range
                    if standard_value['min'] <= int_value <= standard_value['max']:
                        return {'unique_id': standard_value['unique_id'], 'parent_id': standard_value['parent_id']}
                elif standard_value['max'] is not None:
                    # If the standard value has only max range, check if the input value is less than or equal to the max range
                    if int_value <= standard_value['max']:
                        return {'unique_id': standard_value['unique_id'], 'parent_id': standard_value['parent_id']}
                elif standard_value['min'] is not None:
                    # If the standard value has only min range, check if the input value is greater than or equal to the min range
                    if int_value >= standard_value['min']:
                        return {'unique_id': standard_value['unique_id'], 'parent_id': standard_value['parent_id']}
            else:
                # If no mapping is found and bypass is True, return the original value
                if bypass:
                    return {'value': value}
                else:
                    # If no mapping is found and bypass is False, return an empty string
                    return None
    except Exception as e:
        return {'value': value}
        

def value_mapping_of_string(value, values_data, bypass):
    """
    Maps a string value to its corresponding unique_id and parent_id from a given values_data dictionary.

    Args:
        value (str): The string value to be mapped.
        values_data (list[dict]): A list of dictionaries containing the mapping data.
        bypass (bool): If True, returns a dictionary with the original value if it is not found in the mapping_dict.

    Returns:
        dict: A dictionary containing the unique_id and parent_id of the mapped value, or the original value if bypass is True and the value is not found in the mapping_dict.
    """
    try:
        mapping_dict = {item["value"].lower(): {'unique_id': item["unique_id"], 'parent_id': item["parent_id"]} for item in values_data}
        if not value:
            return None
        elif value.lower() in mapping_dict:
            return mapping_dict[value.lower()]
        else:
            if bypass:
                return {'value': value}
            else:
                return None
    except Exception as e:
        return {'value': value}

