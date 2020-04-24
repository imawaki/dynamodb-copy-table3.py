#!/usr/bin/python
import argparse  # {{{ imoprt
import boto3 as boto3
import sys
import os  # }}}


parser = argparse.ArgumentParser(description='Copies dynamodb')  # {{{

parser.add_argument('--src', help='Table name of dynamodb to be cpied from')
parser.add_argument('--dst', help='Table name of destination')
parser.add_argument('-f', '--function_name', help='Name of lambda function to copy. Ex. devIoTExDriverAlexaFunctions')
parser.add_argument('-d', '--dry-run', action='store_true')
parser.add_argument('--dev_to_trial', action='store_true')
parser.add_argument('--trial_to_prod', action='store_true')
parser.add_argument('--show_diff', action='store_true')

args = parser.parse_args()  # }}}


dynamodb = boto3.resource('dynamodb')  # {{{

''' currently unsupported
mynamodb = boto3.resource('dynamodb',
                           aws_access_key_id='',          # put your aws credentials here if you
                           aws_secret_access_key='',      # want to migrate to another aws account
                           region_name='ap-northeast-1')
'''

src_table = dynamodb.Table(args.src)
dst_table = dynamodb.Table(args.dst)  # }}}


def main():  # {{{

    if table_exists(src_table):  # {{{
        print('source table: %s' % src_table.table_arn)
    else:
        print("source table doesn't exist")
        sys.exit(1)  # }}}

    if table_exists(dst_table):  # {{{
        print('destination table: %s' % dst_table.table_arn)
    else:
        print("destination table doesn't exist")
        # create_destination_table(src_table, args.dst)  # }}}

    if args.function_name:
        if args.trial_to_prod:  # {{{
            _from = 'trial'
            _to = 'prod'  # }}}
        if args.dev_to_trial:  # {{{
            _from = 'dev'
            _to = 'trial'  # }}}
        if args.function_name and 'IoTExDriver' in arg.function_name and arg.function_name.endswith('Functions'):  # {{{
            target = 'aws.lambda.' + args.function_name
        else:
            target = 'None'  # }}}
        migrate_function_records(_from, _to, target)

    print ('We are done. Exiting...')  # }}}


def table_exists(table):  # {{{
    result = False
    try:
        result = table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
    except:
        result = False
    return result  # }}}


def create_destination_table(src_table: dynamodb.Table, dst_table_name: str):  # {{{
    try:
        print('Creating %s. Please wait...' % dst_table_name)
        dst_table = dynamodb.create_table(  # {{{
                TableName=dst_table_name,
                KeySchema=src_table.key_schema,
                AttributeDefinitions=src_table.attribute_definitions,
                ProvisionedThroughput={
                    'ReadCapacityUnits': src_table.provisioned_throughput['ReadCapacityUnits'],
                    'WriteCapacityUnits': src_table.provisioned_throughput['WriteCapacityUnits']
                    }
            )  # }}}
        dst_table.wait_until_exists()
        print('Created %s.' % dst_table_name)
    except Exception as e:
        print(e)
        sys.exit(1)  # }}}


def migrate_function_records(_from :str, _to :str, target :str):  # {{{

    with dst_table.batch_writer() as batch:  # {{{
        print('Copying items...')
        print('target_function: %s' % target)
        for item in src_table.scan()['Items']:
            if ((item.get('virtual_driver_command_key', '').startswith(target)) \
                  or (item.get('driver_edge_thing_key', '').startswith(target)) \
                              or (item.get('driver_id', '').startswith(target))):
                for k, v in item.items():
                    if type(v) == str and _from in v:
                        item[k] = v.replace(_from ,_to)

                print(item, end='\n')
                print('', end='\n')

                None if args.dry-run else batch.put_item(Item=item)

        print('finished')  # }}}
    # }}}


main()

