import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from operator import itemgetter

from http import HTTPStatus

def get_items(path, brand):
    dynamodb = boto3.resource('dynamodb')
    
    if brand == 'e5':
        table_entries = dynamodb.Table('entries')
        table_results = dynamodb.Table('results')
        table_workouts = dynamodb.Table('workouts')
    elif brand == 'MC':
        table_entries = dynamodb.Table('entries_mc')
        table_results = dynamodb.Table('results_mc')
        table_workouts = dynamodb.Table('workouts_mc')
    
    message = ''
    try:
        items = []

        filtering_exp = Attr('Brand').eq(brand)

        if path == '/entries':
            response = table_entries.scan(FilterExpression=filtering_exp)
            if response:
                items = sorted(response['Items'], key=itemgetter('Entry_Date'), reverse=True)
            else:
                message = 'Entries Not Found for brand: ' + brand + '.'
                print(f"Dynamodb - Entries Not Found for brand: {brand}")

        if path == '/results':
            response = table_results.scan(FilterExpression=filtering_exp)
            if response['Items']:
                items = sorted(response['Items'], key=itemgetter('Event_Date'), reverse=True)
            else:
                message = 'Results Not Found for brand: ' + brand + '.'
                print(f"Dynamodb - Results Not Found for brand: {brand}")

        if path == '/workouts':
            response = table_workouts.scan(FilterExpression=filtering_exp)
            if response['Items']:
                items = sorted(response['Items'], key=itemgetter('Event_Date'), reverse=True)
            else:
                message = 'Wourkouts Not Found for brand: ' + brand + '.'
                print(f"Dynamodb - Workouts Not Found for brand: {brand}")

        if path == '/events':

            events = {}

            entries_items = table_entries.scan(FilterExpression=filtering_exp)
            entries_list = []
            if entries_items['Items']:
                entries_list = sorted(entries_items['Items'], key=itemgetter('post_time'), reverse=False)

            response = table_workouts.scan(FilterExpression=filtering_exp)
            workouts_list = []
            if response['Items']:
                workouts_list = sorted(response['Items'], key=itemgetter('Event_Date'), reverse=True)

            for elem in entries_list:
                item = {
                    "type": "e",
                    "backgroundColor": "#582C83",
                    "textColor": "#FFFFFF",
                    "horse": elem["Horse_Name"],
                    "entered": elem["Number_Entered"],
                    "track": elem["Track"],
                    "class": elem["Class"],
                    "brand": brand,
                    "post_time": elem["post_time"],
                    "entry_date": elem["Entry_Date"],
                    "jockey_name": elem["jockey_name"],

                }
                key = elem['Entry_Date']
                if key in events.keys():
                    events[key].append(item)
                else:
                    events[key] = [item]

            for elem in workouts_list:
                item = {
                    "type": "w",
                    "backgroundColor": "#44D62C",
                    "textColor": "#FFFFFF",
                    "horse": elem["Horse_Name"],
                    "time": elem["Time"],
                    "track": elem["Track"],
                    "distance": elem["Distance"],
                    "brand": brand,
                }
                key = elem['Event_Date']
                if key in events.keys():
                    events[key].append(item)
                else:
                    events[key] = [item]

            # validate if is not in events, to add and assign it a color because Agenda puts an ugly blue to today
            today = datetime.datetime.today().strftime('%Y-%m-%d')
            if today not in events.keys():
                item = {
                    "type": "empty",
                    "backgroundColor": "#C1C1C1",
                    "textColor": "#000000",
                }
                events[today] = [item]

            items = events

        if items:

            return {
                'status_code': HTTPStatus.OK.value,
                'message': 'Success',
                'items': items
            }

        return {
            #'status_code': HTTPStatus.NOT_FOUND.value,
            'status_code': HTTPStatus.OK.value,
            'message': message,
            'items': []
        }

    except Exception as ex:
        print(ex.__str__())

def lambda_handler(event, context):
    try:

        if not event:
            raise Exception("Event is missing")

        # vars derived from event object
        path = event.get("path", '')

        if event['httpMethod'] == 'GET':

            brand = event["queryStringParameters"].get('brand', 'e5')
            new_entries = event["queryStringParameters"].get('new', False)
            tomorrow_entries = event["queryStringParameters"].get('tomorrow', False)

            response = get_items(path=path, brand=brand)
            print(path)
            print(brand)

        else:
            raise Exception("HTTP Method not supported")

        if response:
            return {
                'statusCode': response['status_code'],
                'body': json.dumps({
                    'items': response['items'],
                    'message': response['message']
                })
            }

    except Exception as ex:
        return {
            'statusCode': HTTPStatus.NOT_FOUND.value,
            'body': json.dumps({
                'error': ex.__str__()
            })
        }

