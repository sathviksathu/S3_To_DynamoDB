import boto3
import pandas as pd
import pytz
import io

aws_session = boto3.Session()
client = aws_session.client('s3', region_name="ap-south-1")
    
def get_data_frame():
    print("s3 main called")
    csv_obj = client.get_object(Bucket="mycsvdb", Key="trends.csv")
    body = csv_obj['Body']
    df = pd.read_csv(io.BytesIO(body.read()))
    return df