# Author: Sergio Olvera
# Date: April 18, 2023

import json
import boto3
import pyodbc
import pandas as pd
from io import StringIO


def getS3Object(key, bucket):
    """
    Connects to S3 Resource and fetches data as a csv file.
    :param key: key to the s3 object
    :param bucket: bucket to the s3 objec
    :return: The csv file containing the s3 key object
    """
    s3 = boto3.client('s3')
    object = s3.get_object(Bucket=bucket, Key=key)
    data = object['Body']
    csv_string = data.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))
    return df
    #return results


def connect():
    """
    Loads json credentials for database & Conects to MSSQL Server.
    :return: new pyodbc connection
    """
    with open('db-credentials.json', 'r') as file:
        creds = json.load(file)
    return pyodbc.connect(f"DRIVER={creds['driver']};PORT={creds['port']};SERVER={creds['server']};DATABASE={creds['database']};UID={creds['username']};PWD={creds['password']}")


def insert(cursor, table, payload):
    for index, row in payload.iterrows():
        if index == 0: #Skipps header in payload
            pass
        values = tuple(row)
        query = f"INSERT INTO {table} VALUES {values}"
        cursor.execute(query)



def dbPayload(cnxn, table, payload, method):
    """
    Injects the payload to an existing table in the database.
    :param table: table to the database
    :param payload: given data payload to be inserted
    :param method: 
        'replace' -> deletes tables contents & inserts payload. 
        'append' -> appends payload to existing payload.
    :return: status of the insertion
    """
    cursor = cnxn.cursor()
    if method == 'replace':
        cursor.execute(f"DELETE FROM {table}")
    insert(cursor, table, payload)

    cnxn.commit()       
    cursor.close()
    cnxn.close()
    return f'{method.upper()} Successful: Contents from {table} altered with payload.'


def validMethod(method):
    valid = ['replace','append']
    return method.lower() if method.lower() in valid else False


def lambda_handler(event, context):
    results = []
    # Get S3 Data from our event handler
    data = event.get("data")
    for event in data:
        table = event.get("table")
        s3_bucket = event.get("s3").get("bucket")
        s3_key = event.get("s3").get("key")
        method = event.get("method")

        # Check if method is valid
        if not validMethod(method):
            return{
                'StatusCode' : 500,
                'error': f"Invalid insertion method '{method}'. Valid methods: {['replace','append']} "
            }
        # Start processing data
        cnxn = connect()
        method = validMethod(method)
        payload = getS3Object(s3_key, s3_bucket)
        result = dbPayload(cnxn, table, payload, method)
        results.append(result)

    return {
        'StatusCode': 200,
        'resutls': results
    }
