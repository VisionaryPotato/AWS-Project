# Author: Sergio Olvera
# Date: April 18, 2023

import logging
import boto3
import pandas as pd
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def uploadToS3(s3_key, s3_bucket, data):
    """
    Use the AWS SDK for Python (Boto3) to create an Amazon Simple Storage Service
    (Amazon S3) resource and puts the desired data object to a specified s3 key & bucket.
    :param s3_key: key to s3 object
    :param s3_bucket: s3 bucket
    :param data: data to be pushed to s3 bucket
    :return: dictionary indicating a successful or unsucessful transaction
    """
    try:
        s3 = boto3.client('s3')
        csv_buffer = data.to_csv(index=False).encode()
        s3.put_object(Body=csv_buffer, Bucket=s3_bucket, Key=s3_key)
        return {"SUCCESS" : f"Successfully put data to S3:{s3_bucket}/{s3_key}."}
    except Exception as e:
        return {"ERROR" : f"Unable to put data to S3:{s3_bucket}/{s3_key}. {e}"}


def googleSheetAPI(sheet_id, sheet, columns):
    """
    Connects to Google Sheets API using google credentials.
    param event: Fetch google sheet ID & google sheet from event handler
    return: Dataframe object containing data specified from google sheet.
    """
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        SERVICE_ACCOUNT_FILE = 'google-credentials.json'
        creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        # Build the service object.
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API to retrieve the data.
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=sheet).execute()
        values = result.get('values', [])

        # Build & transform dataframe
        df = pd.DataFrame(values, columns=columns)
        df.dropna(inplace=True)
        return df
    except Exception as e:
        return {"ERROR": e}
    
def transform_food(df):
    # convert the 'Protein', 'Carbohydrates', 'Fat', and 'Fiber' columns to the 'DECIMAL' data type
    df['Protein'] = pd.to_numeric(df['Protein'], errors='coerce')
    df['Carbohydrates'] = pd.to_numeric(df['Carbohydrates'], errors='coerce')
    df['Fat'] = pd.to_numeric(df['Fat'], errors='coerce')
    df['Fiber'] = pd.to_numeric(df['Fiber'], errors='coerce')

    # convert the 'Servings' and 'Calories' columns to the 'NUMERIC' data type
    df['Servings'] = pd.to_numeric(df['Servings'], errors='coerce')
    df['Calories'] = pd.to_numeric(df['Calories'], errors='coerce')

    # convert the 'Name' and 'Type' columns to the 'VARCHAR' data type
    df['Name'] = df['Name'].astype(str)
    df['Type'] = df['Type'].astype(str)
    df = df.dropna()
    df = df.replace('nan', 'NULL')
    return df


def transform_df(name, dataframe):
    """
    Takes in a dataframe object and the name of the processing object which 
    detemines the type of formatting required.
    :param name: The name specified by the payload
    :param dataframe: dataframe to transform
    :return dataframe: transformed dataframe
    """
    if name.lower() == "food":
        dataframe = transform_food(dataframe)
    #elif name.lower() == "bwtracker":
    #    dataframe = transform_bw(dataframe)
    
    return dataframe



def lambda_handler(event, context):
    """
    :param event: The event dict that contains the parameters sent when the function
                  is invoked.
    :param context: The context in which the function is called.
    :return: The result of the action.
    """
    results = []
    data = event.get("data")
    try: 
        for event in data:
            data_name = event.get("name")
            s3_bucket = event.get("s3").get("bucket")
            s3_key = event.get("s3").get("key")

            sheet = event.get("sheet").get("sheet")
            sheet_id = event.get("sheet").get("id")
            sheet_columns = event.get("sheet").get("schema")
            
            sheet_df = googleSheetAPI(sheet_id, sheet, sheet_columns)
            transformed_df = transform_df(data_name, sheet_df)
            response = uploadToS3(s3_key, s3_bucket, transformed_df)
            results.append(response)
    except Exception as e:
        return {
            "statusCode" : 500,
            "ERROR" : e
            }
    return {
        "statusCode" : 200,
        "reults" : results
        }
