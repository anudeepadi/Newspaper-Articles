import requests as rs
import json
import os
import mysql.connector
import sys
import boto3
import os
import pyspark

# An Data Engineering Project
# This is a simple script to get the data from the API and save it in a file
API_KEY=""
ENDPOINT=""
PORT=""
USER=""
REGION=""
DBNAME=""
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

def get_data():
    url = "https://newsapi.org/v2/everything?q=bitcoin&apiKey={}".format(API_KEY)
    response = rs.get(url)
    data = response.json()
    return data

def get_query(query):
    url = "https://newsapi.org/v2/everything?q={}&apiKey={}".format(query, API_KEY)
    response = rs.get(url)
    data = response.json()
    return data

def save_data(data):
    with open("data.json", "w") as f:
        json.dump(data, f)

def convert_to_parquet():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    df = spark.read.json('data.json')
    df.write.parquet('data.parquet')

def upload_data_to_s3():
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('data.json', '', 'data.json')

def get_data_from_s3():
    s3 = boto3.resource('s3')
    s3.meta.client.download_file('', 'data.json', 'data.json')

def convert_data_to_csv():
    import pandas as pd
    data = pd.read_json('data.json')
    data.to_csv('data.csv', index=False)

def send_to_dynamodb():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('')
    # send data.json to dynamodb with id as primary key
    id = 0
    with open('data.json') as json_file:
       data = json.load(json_file)
       for p in data['articles']:
              id += 1
              id = int(id)
              title = p['title']
              description = p['description']
              url = p['url']
              urlToImage = p['urlToImage']
              publishedAt = p['publishedAt']
              content = p['content']
              table.put_item(
                Item={
                     'id': id,
                     'title': title,
                     'description': description,
                     'url': url,
                     'urlToImage': urlToImage,
                     'publishedAt': publishedAt,
                     'content': content
                }
              )

def db_connection():
    session = boto3.Session(profile_name='default')
    client = session.client('rds', region_name='')
    token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER)
    try:
        conn =  mysql.connector.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, database=DBNAME, ssl_ca='SSLCERTIFICATE')
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e)) 
                
def main():
    data = get_query("SpaceX")
    save_data(data)

if __name__ == "__main__":
    #send_to_dynamodb()
    #convert_data_to_csv()
    convert_to_parquet()