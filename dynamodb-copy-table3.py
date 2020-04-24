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
parser.add_argument('--copy', action='store_true')
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
        print('destination table: %s' % dst_table.table_arn, end='\n')
    else:
        print("destination table doesn't exist", end='\n')
        # create_destination_table(src_table, args.dst)  # }}}

    print('', end='\n')

    if args.trial_to_prod:  # {{{
        _from = 'trial'
        _to = 'prod'  # }}}
    if args.dev_to_trial:  # {{{
        _from = 'dev'
        _to = 'trial'  # }}}

    if args.function_name:
        target = args.function_name
        if 'IoTExDriver' in target and target.endswith('Functions'):  # {{{
            if not args.function_name.startswith('aws.lambda.'):
                target = 'aws.lambda.' + args.function_name
        else:
            target = 'None'  # }}}

    if args.copy:
        migrate_function_records(_from, _to, target)

    if args.show_diff:
        show_diff(_from, _to, target)

    print ('We are done. Exiting...')  # }}}


def table_exists(table :dynamodb.Table) -> bool:  # {{{
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


def show_diff(_from :str, _to :str, target :str):  # {{{

    print('Showing diff between %s and %s' % (target, _to))
    print('', end='\n')

    for item in src_table.scan()['Items']:  # {{{
        if item.get('virtual_driver_command_key', '').startswith(target):
            # k = {'driver_thing_attr_key': item['driver_thing_attr_key'].replace(_from, _to), "service_id":item['service_id']}
            # compare = dst_table.get_item(Key=k)
            pass  # TODO 
            print('#TODO')  # }}}

        if item.get('driver_thing_attr_key', '').startswith(target):  # {{{
            try:
                # print(type(item['available_command']))
                l1 = [ i['command_code'] for i in item['available_command'] ]
            except Exception as e:
                print(item)
                print(str(e))
            if '$virtual_set_cache_value' in l1:
                l1.remove('$virtual_set_cache_value')

            if _from and _to:
                k = {'driver_thing_attr_key': item['driver_thing_attr_key'].replace(_from, _to), "service_id":item['service_id']}
            else:
                k = {'driver_thing_attr_key': item['driver_thing_attr_key'], "service_id":item['service_id']}
            compare = dst_table.get_item(Key=k)
            l2 = []
            if 'Item' in compare:
                l2 = [ i['command_code'] for i in compare['Item']['available_command'] ]
                if '$virtual_set_cache_value' in l2:
                    l2.remove('$virtual_set_cache_value')
            else:
                print('compare not found in %s' % _to)
                print('Key = %s' % k, end='\n')
                print(item,end='\n')
                print('', end='\n')
                continue

            if set(l1) == set(l2):
                print('%s same' % item['driver_thing_attr_key'])
                print('Key = %s' % k, end='\n')
                print('', end='\n')
            else:
                print('%s differ' % item['driver_thing_attr_key'])
                print('Key = %s' % k, end='\n')
                print(item)
                print(compare['Item'])
                print('', end='\n')  # }}}

        if item.get('driver_edge_thing_key', '').startswith(target):  # {{{
            compare = dst_table.get_item(Key={'driver_edge_thing_key': item['driver_edge_thing_key'].replace(_from, _to)})
            print('#TODO')  # }}}

        if item.get('driver_id', '').startswith(target):  # {{{
            compare = dst_table.get_item(Key={'driver_id': item['driver_id'].replace(_from, _to)})
            print('#TODO')
            print(item)
            print(compare)  # }}}

    print('finished')  # }}}


main()

