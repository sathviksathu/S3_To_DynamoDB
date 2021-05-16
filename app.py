import json

import s3_accessor
import dynamoDb_accessor
import os

def handler(event, context):
    # TODO implement
    trends_df = s3_accessor.get_data_frame()
    #for i, row in df.iterrows():
    #    item = row.to_dict()
    
    #create table if it doesn't exist
    gtrends_table = dynamoDb_accessor.get_table()
    #wait for it to get created and print the number of rows in it, expected to be 0.
    #print("No of rows in gtrends table: ")
    #print(dynamoDb_accessor.get_table_status("GTrends"))

    #Insert dataframe rows into table
    dynamoDb_accessor.put_items(gtrends_table, trends_df)
    

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
