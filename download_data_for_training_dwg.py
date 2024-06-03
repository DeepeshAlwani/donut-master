import boto3
import sys
import json
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

def download_file_from_s3(s3_url):
    s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    position = s3_url.find('//', 8) + 2
    object_key = s3_url[position:]
    object_key = urllib.parse.unquote(object_key)
    bucket_name = S3_BUCKET_NAME
    file_obj = io.BytesIO()
    s3_client.download_fileobj(bucket_name, object_key, file_obj)
    file_obj.seek(0)
    filename = object_key.split("/")[-1]
    return file_obj, filename

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

for item in s3url_org_doc_ids:
    pageNum = item["page_num"]
    docId = item["doc_id"]
    titleVal = item["title_val"]
    numVal = item["num_val"]
    file_obj, filename = download_file_from_s3(item["s3Url"])
    pdf_bytes = file_obj.read()
    updated_s3_url = upload_file_to_s3(filename, pdf_bytes, pageNum, docId)
    jpeg_url = create_jpeg_and_upload_to_s3(pdf_bytes, pageNum, docId, filename)
    json_url = create_json_data_and_upload_to_s3(titleVal, numVal, docId, pageNum, filename)
    request = update_data_in_dynamodb(pageNum, docId, updated_s3_url, jpeg_url, json_url)
    print(request)
