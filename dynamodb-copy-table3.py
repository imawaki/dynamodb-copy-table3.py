#!/usr/bin/python
import boto3 as boto3
import sys
import os

if len(sys.argv) != 4:
    print('Usage: %s <source_table_name>' \
        ' <destination_table_name> <target_function_name>' % sys.argv[0])
    sys.exit(1)

region = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-1')

dynamodb = boto3.resource('dynamodb')
mynamodb = boto3.resource('dynamodb',
        aws_access_key_id='',
        aws_secret_access_key='',
        region_name=region
)


src_table = sys.argv[1]
try:
    src_table = dynamodb.Table(src_table)
    print('source table: %s' % src_table.table_arn)
except:
    print("src_table doesn't exit")
    sys.exit(1)


dst_table = sys.argv[2]
try:
    _ = dynamodb.Table(dst_table)
    if _.table_arn is not None:
        print('dst_table already exits.')
        print(_.table_arn)
        sys.exit(1)
except:
    pass


try:  # Create new table
    print('Creating dst_table. Please wait...')
    dst_table = dynamodb.create_table(
            TableName=dst_table,
            KeySchema=src_table.key_schema,
            AttributeDefinitions=src_table.attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': src_table.provisioned_throughput['ReadCapacityUnits'],
                'WriteCapacityUnits': src_table.provisioned_throughput['WriteCapacityUnits']
                }
        )
    dst_table.wait_until_exists()
    print('Created dst_table')
except Exception as e:
    print(e)
    sys.exit(1)

target_function = sys.argv[3]


target_function = 'aws.lambda.' + target_function
with dst_table.batch_writer() as batch:
    print('Copying items...')
    for item in src_table.scan()['Items']:
        if (('virtual_driver_command_key' in item and (not item['virtual_driver_command_key'].startswith(target_function))) \
              or ('driver_edge_thing_key' in item and (not item['driver_edge_thing_key'].startswith(target_function))) \
                          or ('driver_id' in item and (not item['driver_id'].startswith(target_function)))):
            continue

        for k, v in item.items():
            if type(v) == str and 'dev' in v:
                item[k] = v.replace('dev', 'trail')

        print(item, end='\n')
        print('', end='\n')

        batch.put_item(Item=item)

    print('finished')


print ('We are done. Exiting...')
