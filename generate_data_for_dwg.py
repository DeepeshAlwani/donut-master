import boto3
import sys
import json
import requests
import urllib.parse
import io
from PIL import Image
from pdf2image import convert_from_bytes
from boto3.dynamodb.conditions import Attr

DYNAMODB_TABLE_NAME = 'DwgHdrInfo'
AWS_REGION = 'us-east-1'
S3_BUCKET_NAME = 'cverse-dev'
S3_BUCKET_NAME1 = 'cf-ai-test2'

if len(sys.argv) != 3:
    print("Usage: python script.py <AWS_ACCESS_KEY_ID> <AWS_SECRET_ACCESS_KEY>")
    sys.exit(1)

AWS_ACCESS_KEY_ID = sys.argv[1]
AWS_SECRET_ACCESS_KEY = sys.argv[2]

def get_pdf_s3_url():
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, 
                              aws_access_key_id=AWS_ACCESS_KEY_ID, 
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    response = table.scan(
        FilterExpression=Attr('numVal').exists() & Attr('titleVal').exists() & Attr('status').not_exists()
    )

    items = response.get('Items', [])
    s3url_org_doc_ids = []

    for item in items:
        s3url_org_doc_ids.append({
            's3Url': item.get('s3Url'),
            'num_val': item.get('numVal'),
            'title_val': item.get('titleVal'),
            'page_num': item.get('pageNum'),
            'doc_id': item.get('docId')
        })
    return s3url_org_doc_ids

def generate_signed_url(url, expiration=3600):
    print(url)
    position = url.find('//', 8) + 2  # Adding 2 to include the double slashes
    object_key = url[position:]
    print("Extracted object key:", object_key)
    object_key = urllib.parse.unquote(object_key)
    print("Decoded object key:", object_key)

    s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Generate a pre-signed URL for the S3 object
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': S3_BUCKET_NAME,
            'Key': object_key,
        },
        ExpiresIn=expiration
    )
    filename = object_key.split("/")[-1]
    return presigned_url, filename

def upload_file_to_s3(filename, data, pageNum, docId):
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_object_key = f"drawing/{docId}/{pageNum}/pdf/{filename}"
    s3.put_object(Bucket=S3_BUCKET_NAME1, Key=s3_object_key, Body=data)
    updated_s3_url_presigned = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET_NAME1, 'Key': s3_object_key},
                        ExpiresIn=5 * 365 * 24 * 60 * 60 
                    )
    updated_s3_url = f"https://{S3_BUCKET_NAME1}.s3.amazonaws.com/{s3_object_key}"
    return updated_s3_url

def create_json_data_and_upload_to_s3(titleVal, numVal, docId, pageNum, filename):
    data = {
        "header": {
            "drawingTitle": titleVal,
            "drawingNumber": numVal
        }
    }

    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Prepare the filename and S3 client
    filename = filename.split(".")[0]
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Construct the S3 object key
    s3_object_key = f"drawing/{docId}/{pageNum}/json/{filename}.json"
    
    # Upload the JSON data to S3
    s3.put_object(Bucket=S3_BUCKET_NAME1, Key=s3_object_key, Body=json_data)
    
    # Generate a presigned URL
    json_url_presigned = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET_NAME1, 'Key': s3_object_key},
        ExpiresIn=5 * 365 * 24 * 60 * 60 
    )
    
    # Construct the updated S3 URL
    json_url = f"https://{S3_BUCKET_NAME1}.s3.amazonaws.com/{s3_object_key}"
    print(json_url_presigned)
    return json_url

def create_jpeg_and_upload_to_s3(pdf_bytes, pageNum, docId, filename):
    pageNum = int(pageNum)
    
    # Convert the specific page to an image (pageNum starts from 0, pdf2image expects 1-based index)
    images = convert_from_bytes(pdf_bytes, first_page=pageNum + 1, last_page=pageNum + 1)
    image = images[0]
    
    # Convert the image to JPEG format
    jpeg_bytes = io.BytesIO()
    image.save(jpeg_bytes, format="JPEG")
    jpeg_bytes.seek(0)
    
    filename = filename.split(".")[0]
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Construct the S3 object key
    s3_object_key = f"drawing/{docId}/{pageNum}/jpeg/{filename}.jpg"
    
    # Upload the JPEG data to S3
    s3.put_object(Bucket=S3_BUCKET_NAME1, Key=s3_object_key, Body=jpeg_bytes)
    
    # Generate a presigned URL
    jpeg_url_presigned = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET_NAME1, 'Key': s3_object_key},
        ExpiresIn=5 * 365 * 24 * 60 * 60
    )
    
    # Construct the updated S3 URL
    jpeg_url = f"https://{S3_BUCKET_NAME1}.s3.amazonaws.com/{s3_object_key}"
    
    print(jpeg_url_presigned)

    return jpeg_url

def update_data_in_dynamodb(pageNum, docId, updated_s3_url, jpeg_url, json_url):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    response = table.update_item(
        Key={
            'pageNum': pageNum,
            'docId': docId,
        },
        UpdateExpression='SET updatedS3Url = :updatedS3Url, jpegUrl = :jpegUrl, jsonUrl = :jsonUrl, #status = :status',
        ExpressionAttributeValues={
            ':updatedS3Url': updated_s3_url,
            ':jpegUrl': jpeg_url,
            ':jsonUrl': json_url,
            ':status': 'notUsedForTraining'
        },
        ExpressionAttributeNames={
            '#status': 'status'  # 'status' is a reserved keyword in DynamoDB, so using ExpressionAttributeNames
        },
        ReturnValues='UPDATED_NEW'
    )

    return response

s3url_org_doc_ids = get_pdf_s3_url()
if len(s3url_org_doc_ids) < 1:
    print("No records to fetch. Exiting the script.")
    exit()

for dict in s3url_org_doc_ids:
    pageNum = dict["page_num"]
    docId = dict["doc_id"]
    titleVal = dict["title_val"]
    numVal = dict["num_val"]
    signed_url, filename = generate_signed_url(dict["s3Url"])
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(signed_url, headers=headers)
        response.raise_for_status()
        updated_s3_url = upload_file_to_s3(filename, response.content, pageNum, docId)
        pdf_bytes_io = io.BytesIO(response.content)
        pdf_bytes = pdf_bytes_io.read()
        jpeg_url = create_jpeg_and_upload_to_s3(pdf_bytes, pageNum, docId, filename)
        json_url = create_json_data_and_upload_to_s3(titleVal, numVal, docId, pageNum, filename)
        request = update_data_in_dynamodb(pageNum, docId, updated_s3_url, jpeg_url, json_url)
        print(request)
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print("Response content:", response.content)
    except Exception as e:
        print(f"Failed to complete the request: {e}")
