import pandas as pd
import json
import os

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
    df_filtered = df[df['salary'] > 50000]

    return df_filtered



def load_data_to_csv(df, output_path):
    print(f"Loading data to CSV file: {output_path}")
    df.to_csv(output_path, index=False)

def load_data_to_json(df, output_path):
    print(f"Loading data to JSON file: {output_path}")
    df.to_json(output_path, orient='records', lines=True)


def main():

    csv_data = extract_data_from_csv('../data/input/dataset1.csv')
    json_data = extract_data_from_json('../data/input/dataset.json')
    df=csv_data

    # saving df to another csv without index
    csv_data.to_csv('../data/output/transform_index_false.csv',index=False)

    # last few lines at tail, head
    print(csv_data.tail(2))

    # mean, median, mode, max, min : statistics
    print(csv_data.describe())

    # column changes
    # df['name'][4]='Jessus'
    print(df['name'][4])

    # type i.e Series or DF
    print(type(df['id']))

    # Range of index, columns
    print(df.columns)
    print(df.index)
    print(df.head())

    # Sorting
    df=df.sort_index(axis=1,ascending=False)
    print(df)
    df=df.sort_index(axis=1,ascending=True)
    print(df)

    # renaming
    df=df.rename(columns={'name':'emp_name'})
    print(df.head(2))

    # copy fn, using the view,  dropping the null lines
    df2=df.copy()
    df2.loc[0,'age']=1234
    df2.loc[0,0]='rtyui'
    df2=df2.dropna()
    print(df2.head())

    # dropping a column or row
    df2.drop('salary',axis=1,inplace = True)  # column
    # df2.drop([2])   # row
    print(df2)

    # filtering with conditions (loc, iloc)
    df3=df.copy()
    df3=df3.loc[(df['performance_score']<90) & (df3['salary']>46000)]
    df3.loc[2,'age'] = 67
    print(df3.iloc[[0,2],[1,2]])
    print(df3)

    # df=df.max(axis=1)
    # print(df)

    # pivot table
    df=df.drop_duplicates()
    # df=df.pivot(index="salary",columns="age")
    print(df)

















    print("Space hai bhaiya")
    # print the df file





    # combined_data = pd.concat([csv_data, json_data], ignore_index=True).drop_duplicates()
    combined_data=csv_data.drop_duplicates()


    transformed_data = transform_data(combined_data)


    load_data_to_csv(transformed_data, '../data/output/transformed_data.csv')
    load_data_to_json(transformed_data, '../data/output/transformed_data.json')
    print("ETL process completed")

if __name__ == "__main__":
    main()
