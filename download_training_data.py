import boto3
import os
from botocore.exceptions import NoCredentialsError
from collections import defaultdict
import requests
import sys

# Define global variables
table_name = 'AnnotationsInfo'
column_name = 'vendorName'
AWS_REGION = 'us-east-1'
download_folder = 'downloadfiles'
S3_BUCKET_NAME = 'cf-ai-test2'
if len(sys.argv) != 3:
    print("Usage: python script.py <json_path>")
    sys.exit(1)
AWS_ACCESS_KEY_ID = sys.argv[1]
AWS_SECRET_ACCESS_KEY = sys.argv[2]

print(f"arg1 = {AWS_ACCESS_KEY_ID} arg2={AWS_SECRET_ACCESS_KEY}")

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
        if count > 5:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='vendorName = :vendor',
                ExpressionAttributeValues={':vendor': {'S': vendor}}
            )
            urls_dict[vendor] = []
            items = response.get('Items', [])
            for item in items:
                s3_url = item.get('s3Url', {}).get('S')
                json_url = item.get('jsonUrl', {}).get('S')
                urls_dict[vendor].append({'s3Url': s3_url, 'jsonUrl': json_url})

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
            presigned_url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_url},
                                          ExpiresIn=3600)
            response = requests.get(presigned_url)
            filename = s3_url.split('/')[-1]  # Get the filename from the URL
            file_path = os.path.join(download_folder, folder_name)
            os.makedirs(file_path, exist_ok=True)
            file_path = os.path.join(file_path, filename )
            # Download the file from S3
            with open(file_path, 'wb') as f:
                f.write(response.content)

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
            presigned_url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_url},
                                          ExpiresIn=3600)
            response = requests.get(presigned_url)
            filename = s3_url.split('/')[-1]  # Get the filename from the URL
            file_path = os.path.join(download_folder, folder_name)
            os.makedirs(file_path, exist_ok=True)
            file_path = os.path.join(file_path, filename)
            # Download the file from S3
            with open(file_path, 'wb') as f:
                f.write(response.content)

# Example usage
urls_dict = count_entries_by_vendor()
for vendor, urls_list in urls_dict.items():
    print(f"Vendor: {vendor}")
    for urls in urls_list:
        print(f"s3Url: {urls['s3Url']}, jsonUrl: {urls['jsonUrl']}")

if __name__ == "__main__":



    download_jpeg_files(urls_dict, 'images')

    download_json_files(urls_dict, 'json')
