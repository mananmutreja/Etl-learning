import pandas as pd
import json
import os


print("Current working directory:", os.getcwd())


def extract_data_from_csv(file_path):
    print(f"Extracting data from {file_path}")
    return pd.read_csv(file_path)

def extract_data_from_json(file_path):
    print(f"Extracting data from {file_path}")
    with open(file_path) as f:
        data = json.load(f)
    return pd.DataFrame(data)


def transform_data(df):
    print("Transforming data")
    # Example transformation: Filter records with salary > 50000

    # df_filtered['salary'] = df['salary'].astype('float')
    # df.rename(columns={'name':'employee_name'},inplace=True)

    df=df.rename(columns={'name': 'employee_name'})
    # df = df.rename(columns={'name': 'employee_name'})  # Avoid inplace=True
    print("hnn haii")

    df_filtered = df[df['salary'] > 50000]


    # df_filtered['salary_in_k'] = df_filtered['salary'] / 1000

    # df_filtered=df.dropna(inplace=True)

    return df_filtered



def load_data_to_csv(df, output_path):
    print(f"Loading data to CSV file: {output_path}")
    df.to_csv(output_path, index=False)

def load_data_to_json(df, output_path):
    print(f"Loading data to JSON file: {output_path}")
    df.to_json(output_path, orient='records', lines=True)


def etl_process():


    csv_data = extract_data_from_csv('../data/input/dataset1.csv')
    json_data = extract_data_from_json('../data/input/dataset.json')


    print("Combining data")
    # combined_data = pd.concat([csv_data, json_data], ignore_index=True).drop_duplicates()
    combined_data=csv_data.drop_duplicates()


    transformed_data = transform_data(combined_data)


    load_data_to_csv(transformed_data, '../data/output/transformed_data.csv')
    load_data_to_json(transformed_data, '../data/output/transformed_data.json')
    print("ETL process completed")

etl_process()
