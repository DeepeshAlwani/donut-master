import boto3
import os
from botocore.exceptions import NoCredentialsError
from collections import defaultdict
import requests
import sys
import shutil
import json
from urllib.parse import urlparse

# Define global variables
table_name = 'AnnotationsInfo'
column_name = 'vendorName'
AWS_REGION = 'us-east-1'
download_folder = 'downloadfiles'
S3_BUCKET_NAME = 'cf-ai-test2'
if len(sys.argv) != 4:
    print("Usage: python script.py <json_path>")
    sys.exit(1)
AWS_ACCESS_KEY_ID = sys.argv[1]
AWS_SECRET_ACCESS_KEY = sys.argv[2]
THRESHOLD = int(sys.argv[3])

def update_status_in_dynamodb(org_id, annotation_key, status):
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    response = dynamodb.update_item(
        TableName=table_name,
        Key={'orgId': {'S': org_id}, 'annotationKey': {'S': annotation_key}},
        UpdateExpression='SET #status = :status',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':status': {'S': f'{status}'}}
    )

    print(f"Updated status for orgId {org_id} and annotationKey {annotation_key}")

def count_entries_by_vendor():
    # Create a DynamoDB client with specified credentials
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Scan the table to get all items
    response = dynamodb.scan(TableName=table_name)

    # Count the entries for each vendor
    vendor_counts = defaultdict(int)
    for item in response['Items']:
        vendor = item.get(column_name, {}).get('S')
        if vendor:
            vendor_counts[vendor] += 1

    urls_dict = {}
    for vendor, count in vendor_counts.items():
        if count > THRESHOLD:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='vendorName = :vendor and #status = :status and orgId = :orgId',
                ExpressionAttributeValues={':vendor': {'S': vendor}, ':status': {'S': 'notUsedForTraining'}, ':orgId': {'S': '00DHo000002ekTWMAY'}},
                ExpressionAttributeNames={'#status': 'status'}
            )
            urls_dict[vendor] = []
            items = response.get('Items', [])
            for item in items:
                s3_url = item.get('s3Url', {}).get('S')
                json_url = item.get('jsonUrl', {}).get('S')
                annotation_key = item.get('annotationKey', {}).get('S')
                org_id = item.get('orgId', {}).get('S')
                urls_dict[vendor].append({'s3Url': s3_url, 'jsonUrl': json_url, 'orgId': org_id, 'annotationKey': annotation_key})

    return urls_dict

def download_jpeg_files(urls_dict, folder_name):
    # Create a new S3 client
    s3 = boto3.client('s3', region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Create the download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Download files for each vendor
    for vendor, urls_list in urls_dict.items():
        for urls in urls_list:
            s3_url = urls['s3Url']
            # Determine if the input is a URL or an object key
            if s3_url.startswith("http://") or s3_url.startswith("https://"):
                # Parse the URL
                parsed_url = urlparse(s3_url)
                key = parsed_url.path.lstrip('/')
            else:
                # Assume the input is an object key
                key = s3_url

            presigned_url = s3.generate_presigned_url('get_object',
                                                      Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                                                      ExpiresIn=360)
            response = requests.get(presigned_url)
            filename = key.split('/')[-1]  # Get the filename from the key
            file_path = os.path.join(download_folder, folder_name)
            os.makedirs(file_path, exist_ok=True)
            file_path = os.path.join(file_path, filename)
            # Download the file from S3
            print(presigned_url)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            update_status_in_dynamodb(urls['orgId'], urls['annotationKey'], status='Complete')

def download_json_files(urls_dict, folder_name):
    # Create a new S3 client
    s3 = boto3.client('s3', region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Create the download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Download files for each vendor
    for vendor, urls_list in urls_dict.items():
        for urls in urls_list:
            s3_url = urls['jsonUrl']
            if s3_url.startswith("http://") or s3_url.startswith("https://"):
                # Parse the URL
                parsed_url = urlparse(s3_url)
                key = parsed_url.path.lstrip('/')
            else:
                # Assume the input is an object key
                key = s3_url  # Get the key from the URL
            presigned_url = s3.generate_presigned_url('get_object',
                                            Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                                            ExpiresIn=360)
            response = requests.get(presigned_url)
            filename = s3_url.split('/')[-1]  # Get the filename from the URL
            file_path = os.path.join(download_folder, folder_name)
            os.makedirs(file_path, exist_ok=True)
            file_path = os.path.join(file_path, filename)
            # Download the file from S3
            with open(file_path, 'wb') as f:
                f.write(response.content)

def key_file_checker(urls_dict):
    json_list_path = 'downloadfiles/json'
    jpeg_list_path = 'downloadfiles/images'
    json_file_list = os.listdir(json_list_path)
    jpeg_file_list = os.listdir(jpeg_list_path)
    for file in json_file_list:
        json_path = os.path.join(json_list_path, file)
        with open(json_path, 'r') as f:
            data = json.load(f)

        # Check and rename key 'item' to 'items'
        if 'item' in data:
            data['items'] = data.pop('item')

        # Perform the checks and update status if necessary
        if len(data['header']) < 3:
            for vendor, urls_list in urls_dict.items():
                for urls in urls_list:
                    if file[:-5] in urls['annotationKey']:
                        orgId = urls['orgId']
                        annotationKey = urls['annotationKey']
                        status = 'Failed'
                        update_status_in_dynamodb(orgId, annotationKey, status)
                        for jpeg in jpeg_file_list:
                            if file[:-5] in jpeg:
                                jpeg_path = os.path.join(jpeg_list_path, jpeg)
                                os.remove(jpeg_path)
                                os.remove(json_path)
        for item in data['items']:
            if len(item) < 4:
                for vendor, urls_list in urls_dict.items():
                    for urls in urls_list:
                        if file[:-5] in urls['annotationKey']:
                            orgId = urls['orgId']
                            annotationKey = urls['annotationKey']
                            status = 'Failed'
                            update_status_in_dynamodb(orgId, annotationKey, status)
                            for jpeg in jpeg_file_list:
                                if file[:-5] in jpeg:
                                    jpeg_path = os.path.join(jpeg_list_path, jpeg)
                                    os.remove(jpeg_path)
                                    os.remove(json_path)

        # Save the updated JSON back to the file
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=4)

# Example usage
urls_dict = count_entries_by_vendor()
if len(urls_dict) < 1:
    print("No records to fetch, exiting the script")
    exit()
for vendor, urls_list in urls_dict.items():
    print(f"Vendor: {vendor}")
    for urls in urls_list:
        print(f"s3Url: {urls['s3Url']}, jsonUrl: {urls['jsonUrl']}, annotationKey: {urls['annotationKey']}")

download_jpeg_files(urls_dict, 'images')
download_json_files(urls_dict, 'json')
key_file_checker(urls_dict)
