import boto3
import requests
import os
import sys
from boto3.dynamodb.conditions import Attr

if len(sys.argv) != 3:
    print("Usage: python script.py <json_path>")
    sys.exit(1)
AWS_ACCESS_KEY_ID = sys.argv[1]
AWS_SECRET_ACCESS_KEY = sys.argv[2]
AWS_REGION = 'us-east-1'
TABLE_NAME = 'DwgHdrInfo'
download_folder = r"download"
S3_BUCKET_NAME = 'cf-ai-test2'

def fetch_urls_from_dynamodb():
    # Initialize a session using Amazon DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION,
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Select your DynamoDB table
    table = dynamodb.Table(TABLE_NAME)
    
    # Scan the table for items with status = 'notUsedForTraining'
    response = table.scan(
        FilterExpression=Attr('status').eq('notUsedForTraining')
    )

    
    # Extract jpegUrl and jsonUrl from the items
    urls = [{'jpegUrl': item['jpegUrl'], 'jsonUrl': item['jsonUrl'], 'pageNum': item['pageNum'],'docId': item['docId']} for item in response['Items']]
    
    return urls

def update_status_in_dynamodb(pageNum, docId, status):
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    response = dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={'pageNum': {'S': pageNum}, 'docId': {'S': docId}},
        UpdateExpression='SET #status = :status',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':status': {'S': f'{status}'}}
    )

    print(f"Updated status for pageNum {pageNum} and docId {docId}")

def download_jpeg_files(urls, folder_name):
    try:

    # Create a new S3 client
        s3 = boto3.client('s3', region_name=AWS_REGION,
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # Create the download folder if it doesn't exist
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        # Download files for each vendor
        s3_url = urls['jpegUrl']
        key = '/'.join(s3_url.split('/')[3:])
        print(f"this is the key of the json we are trying to download: {key}")  # Get the key from the URL
        presigned_url = s3.generate_presigned_url('get_object',
                                        Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                                        ExpiresIn=360)
        response = requests.get(presigned_url)
        filename = s3_url.split('/')[-1]  # Get the filename from the URL
        file_path = os.path.join(download_folder, folder_name)
        os.makedirs(file_path, exist_ok=True)
        file_path = os.path.join(file_path, filename)
        # Download the file from S3
        print(presigned_url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        update_status_in_dynamodb(urls['pageNum'], urls['docId'], status = 'Complete')
    except Exception as e:
        print(e)
def download_json_files(urls, folder_name):
    # Create a new S3 client
    s3 = boto3.client('s3', region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Create the download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Download files for each vendor
    s3_url = urls['jsonUrl']
    key = '/'.join(s3_url.split('/')[3:])
    print(key)  # Get the key from the URL
    presigned_url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': S3_BUCKET_NAME, 'Key': key},
                                    ExpiresIn=360)
    response = requests.get(presigned_url)
    filename = s3_url.split('/')[-1]
    print(filename)  # Get the filename from the URL
    file_path = os.path.join(download_folder, folder_name)
    os.makedirs(file_path, exist_ok=True)
    file_path = os.path.join(file_path, filename)
    # Download the file from S3
    with open(file_path, 'wb') as f:
        f.write(response.content)


urls = fetch_urls_from_dynamodb()
if len(urls) <1:
    print("Dictionary is empty. Stopping the script.")
    exit()
for dic in urls:
    #print(dic["jpegUrl"])
    download_jpeg_files(dic, 'images')
    download_json_files(dic, 'json')
    