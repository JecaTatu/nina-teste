import boto3
import pandas as pd
from boto3 import client
from pyathena import connect
from pyathena.util import as_pandas

#Creating Bucket
region='us-east-1'
s3_client = boto3.client('s3', region_name=region)
bucket_name = 'teste-nina'
location = {'LocationConstraint': region}
s3_client.create_bucket(Bucket=bucket_name)

s3 = boto3.resource('s3')
bucket = s3.Bucket('teste-nina')
account_id = boto3.client('sts').get_caller_identity().get('Account')

#Getting the csv to use as a dataset
csv_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/06-18-2020.csv'
df = pd.read_csv(csv_url)   

#Removing columns not so useful for now
df = df.loc[:, ['Province_State', 'Country_Region', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Incidence_Rate', 'Case-Fatality_Ratio']]

#Filtering to just show the Brazil's numbers
df = df.query('Country_Region.str.contains("Brazil")')

#Setting the types for each column
df.loc[:, 'Confirmed'] = df['Confirmed'].astype(int)
df.loc[:, 'Deaths'] = df['Deaths'].astype(int)
df.loc[:, 'Recovered'] = df['Recovered'].astype(int)
df.loc[:, 'Active'] = df['Active'].astype(int)
df.loc[:, 'Province_State'] = df['Province_State'].astype(str).str.lower()
df.loc[:, 'Country_Region'] = df['Country_Region'].astype(str).str.lower()
df.loc[:, 'Incidence_Rate'] = df['Incidence_Rate'].astype(float)
df.loc[:, 'Case-Fatality_Ratio'] = df['Case-Fatality_Ratio'].astype(float)
# print(df)

df.to_csv('covid-brasil-data.csv', index=False, header=False)

#Uploading the csv to s3
bucket.upload_file('./covid-brasil-data.csv', 'covid-brasil-report.csv')


#Starting to make the csv queryable
glue_client = boto3.client('glue')

#Creating the database
database_name = 'nina-teste'
glue_client.create_database(CatalogId=account_id, DatabaseInput={'Name': database_name, 'Description': 'Database with covid informations'})

location_data = 's3://teste-nina/'
table_name = 'Covid Data'

#Creating the table for the dataset
response = glue_client.create_table(
    CatalogId=account_id,
    DatabaseName=database_name,
    TableInput={
        'Name': table_name,
        'Description': 'Covid informations of Brazil',
        'StorageDescriptor': {
            'Columns': [
                {
                    'Name': 'Province_State',
                    'Type': 'string',
                    'Comment': 'Name of the State'
                },         
                {
                    'Name': 'Country_Region',
                    'Type': 'string',
                    'Comment': 'Name of the Country'
                },
                {
                    'Name': 'Confirmed',
                    'Type': 'int',
                    'Comment': 'Number of confirmed cases'
                },
                {
                    'Name': 'Deaths',
                    'Type': 'int',
                    'Comment': 'Number of deaths cases'
                },
                {
                    'Name': 'Recovered',
                    'Type': 'int',
                    'Comment': 'Number of recovered cases'
                },
                {
                    'Name': 'Active',
                    'Type': 'int',
                    'Comment': 'Number of active cases'
                },
                {
                    'Name': 'Incidence_Rate',
                    'Type': 'float',
                    'Comment': 'Rate of incidence'
                },
                {
                    'Name': 'Case-Fatality_Ratio',
                    'Type': 'float',
                    'Comment': 'Percentage of letal case'
                },
            ],
            'Location': location_data,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {
                    'escapeChar': '\\',
                    'separatorChar': ',',
                    'serialization.format': '1'
                }
            },
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'csv'
        }
    }
)


#Making a query 
cursor = connect(region_name=region, s3_staging_dir='s3://teste-nina/').cursor()
cursor.execute('SELECT * FROM "nina-teste"."covid data" limit 10;'.format(database_name, table_name))

df_sql = as_pandas(cursor)

# print(df_sql.head(5))

#Creating aggregation dataset fot the total Deaths, Total cases, Total of recovers and all that still active
df_agg = df.agg({'Confirmed': ['sum'], 'Deaths': ['sum'], 'Recovered': ['sum'], 'Active': ['sum']})
# print(df_agg)

#Calculating the letality of covid in Brazil
def calc_letality(row):
    return row['Deaths'] / row['Confirmed'] * 100

df_agg['Letality'] = df_agg.apply(calc_letality, axis=1)
# print(df_agg)

df_agg.to_csv('covid-brasil-agg.csv', index=False, header=False)

#Creating a new bucket to upload the aggregation
bucket_name_agg = 'teste-nina-agg'
s3_client.create_bucket(Bucket=bucket_name_agg)
bucket_agg = s3.Bucket('teste-nina-agg')

#Uploading the csv to s3
bucket_agg.upload_file('./covid-brasil-agg.csv', 'covid-brasil-agg-report.csv')

table_name_agg = 'aggregation'
location_data_agg = 's3://teste-nina-agg/'

#Creating the new table for the aggregation
response = glue_client.create_table(
    CatalogId=account_id,
    DatabaseName=database_name,
    TableInput={
        'Name': table_name_agg,
        'Description': 'Covid informations of Brazil',
        'StorageDescriptor': {
            'Columns': [
                {
                    'Name': 'Confirmed',
                    'Type': 'int',
                    'Comment': 'Total number of confirmed cases in Brazil'
                },
                {
                    'Name': 'Deaths',
                    'Type': 'int',
                    'Comment': 'Total number of deaths cases in Brazil'
                },
                {
                    'Name': 'Recovered',
                    'Type': 'int',
                    'Comment': 'Total number of recovered cases in Brazil'
                },
                {
                    'Name': 'Active',
                    'Type': 'int',
                    'Comment': 'Total number of active cases in Brazill'
                },
                {
                    'Name': 'Letality',
                    'Type': 'float',
                    'Comment': 'Letality of covid in Brazil'
                },
            ],
            'Location': location_data_agg,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {
                    'escapeChar': '\\',
                    'separatorChar': ',',
                    'serialization.format': '1'
                }
            },
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'csv'
        }
    }
)

#Making a query in the aggregation
cursor = connect(region_name=region, s3_staging_dir='s3://teste-nina/').cursor()
cursor.execute('SELECT * FROM "nina-teste"."covid aggregation data" limit 10;'.format(database_name, table_name_agg))

df_sql_agg = as_pandas(cursor)

# print(df_sql_agg.head(5))