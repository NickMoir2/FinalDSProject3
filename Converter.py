#Final Project
#Data Science Systems 2002
#Nick Moir and Matthew Gottfried
#12/4/2024

import pandas as pd
import json
import requests
from sqlalchemy import create_engine


def extract_data(source, source_type='file', file_format='csv'):  # This is the method for pulling the data from the input source, which varies based on type provided by user.
    try:
        if source_type == 'url':
            response = requests.get(source)  # This is for fetching the data from the website source.
            response.raise_for_status()
            if file_format == 'json':  # This is for if the user provides file type as JSON, where it uses pandas to extract the data.
                data = pd.DataFrame(response.json())
            elif file_format == 'csv':  # This does the same thing, using pandas for CSV instead.
                data = pd.read_csv(source)
            else:
                raise ValueError("Unsupported file format")  # Raises an exception for file types that are not either JSON or CSV.
        elif source_type == 'file':  # This and the following few methods do the same thing, but for a file instead of a link.
            if file_format == 'csv':
                data = pd.read_csv(source)
            elif file_format == 'json':
                with open(source, 'r') as f:
                    data = pd.DataFrame(json.load(f))
            else:
                raise ValueError("Unsupported file format")
        else:
            raise ValueError("Unsupported source type")  # Shows an error if the source type is not valid, ie. File or URL.
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")  # If the data isn't processed from the source, it throws an error and explains the exception.
        return None


def transform_data(data, output_format='csv', sql_table_name=None):  # This is for converting data from one type to another based on user input.
    try:
        if output_format == 'csv':  # Converts the data from the extract_data method into the type of choosing.
            data.to_csv('output.csv', index=False)
        elif output_format == 'json':  #
            data.to_json('output.json', orient='records')
        elif output_format == 'sql':
            if not sql_table_name:  # A db file requires a table name, and throws an error if one isn't provided.
                raise ValueError("sql_table_name must be provided when output_format is 'sql'")
            engine = create_engine('sqlite:///output.db')  # Creates the database.
            data.to_sql(sql_table_name, con=engine, index=False, if_exists='replace')  # Puts data in the dataframe with tablename provided by user.
        else:
            raise ValueError("Unsupported output format")  # If the output provided is not one of the allowed options.
        print(f"Data successfully converted to {output_format}")  # Success statement if it works, helps with error checking.
    except Exception as e:
        print(f"Error converting data: {e}")  # Same error converting data warning as earlier.


def modify_data_columns(data, columns_to_keep=None, new_columns=None):  # Used to modify the columns to remove data and add new data if needed.
    try:
        if columns_to_keep:
            data = data[columns_to_keep]  # Only keeps the columns specified by user.
        if new_columns:
            for col_name, default_value in new_columns.items():
                data[col_name] = default_value  # Just adds new columns at the end of the data with the provided data.
        return data
    except Exception as e:
        print(f"Error modifying columns: {e}")  # If it throws an exception, it will tell us which error was produced.
        return data


def summarize_data(data, title='Data Summary'):  # Simple method to summarize data, literally just prints the total number of data cells and columns.
    print(f"{title}:")
    print(f"Number of records: {len(data)}")
    print(f"Number of columns: {len(data.columns)}")


def etl_pipeline(source, source_type, file_format, output_format, sql_table_name=None, columns_to_keep=None, new_columns=None):  # Main method that takes the user input and calls all the above methods to successfully convert the data.
    try:
        data = extract_data(source, source_type, file_format)  # Gets data from source.
        if data is None:
            return
        summarize_data(data, title='Original Data Summary')  # Prints summary of data pre-alteration.
        data = modify_data_columns(data, columns_to_keep, new_columns)  # Modifies the data.
        summarize_data(data, title='Processed Data Summary')  # Prints summary of data post-alteration.
        transform_data(data, output_format, sql_table_name)  # Converts the data into the new specified format.
    except Exception as e:
        print(f"Error in ETL pipeline: {e}")  # Prints the error if one is thrown during runtime.


def get_user_inputs():  # Method to get all the arguments for the etl_pipeline method. Usages explained in input text.
    source = input("Enter the source file path or URL (Make sure file path is in the working directory if choosing file path): ")
    source_type = input("Is the source a 'file' or 'url'? ")
    file_format = input("Enter the file format ('csv' or 'json'): ")
    output_format = input("Enter the output format ('csv', 'json', or 'sql'): ")

    if output_format == 'sql':  # Only asks for table name if output is an SQL table.
        sql_table_name = input("Enter the SQL table name: ")
    else:
        sql_table_name = None

    columns_to_keep = input("Enter the columns to keep (comma separated), or leave blank to keep all: ").split(',')  # Asks user to specify which columns to keep.
    if not columns_to_keep[0]:
        columns_to_keep = None

    new_columns_input = input("Enter any new columns and default values (e.g., 'status:active'), or leave blank: ")  # Asks user for new data to add to the table.
    if new_columns_input:
        new_columns = dict(item.split(':') for item in new_columns_input.split(','))
    else:
        new_columns = None

    return source, source_type, file_format, output_format, sql_table_name, columns_to_keep, new_columns  # Returns all the inputs.


source, source_type, file_format, output_format, sql_table_name, columns_to_keep, new_columns = get_user_inputs()
etl_pipeline(source, source_type, file_format, output_format, sql_table_name, columns_to_keep, new_columns)  # Calls the main etl_pipeline method using all the user provided data.