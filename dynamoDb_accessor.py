import boto3


def get_table(dynamodb=None):
    print("DynamoDB: Craeting table")
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

    gtrends_table = dynamodb.Table("GTrends")
    if gtrends_table is None:
        table = dynamodb.create_table(
            TableName='GTrends',
            KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'query',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'query',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        return table
    return gtrends_table

def get_table_status(table_name):
    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    return (table.item_count)

def put_items(table, df):
    print("DynamoDB put_items called")
    # for i, row in df.iterrows():
    #     item = row.to_dict()
    #     table.put_item(Item=item)
    #     #print("Inserted item: "+item['year']+" "+item['query'])
    with table.batch_writer() as batch:
    for i, row in df.iterrows():
        batch.put_item(Item=row.to_dict())
    print("DynamoDB put_items completed")
    