import os
import sys
import zipfile
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

if len(sys.argv) <4:
    print("Please provide all the details")

S3_BUCKET_NAME = 'cf-ai-test2'
AWS_ACCESS_KEY_ID = sys.argv[1]
AWS_SECRET_ACCESS_KEY = sys.argv[2]
AWS_REGION = 'us-east-1'

model_name = sys.argv[3]

def zip_files(folder_path, zip_name):
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder_path))

def upload_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client('s3', region_name=AWS_REGION,
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        s3_client.upload_file(file_name, bucket, object_name or file_name)
        print(f"File {file_name} uploaded to {bucket}/{object_name or file_name}")
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
    except NoCredentialsError:
        print("Credentials not available")

def get_s3_url(bucket, object_name):
    url = f"https://{bucket}.s3.amazonaws.com/{object_name}"
    return url

current_date = datetime.now().strftime('%d-%b-%Y')

folder_path = f"fine_tuned_model"
zip_name = f"{model_name}.zip"

# Create the folder structure with the current date
s3_folder = f'testing/models/donut/{current_date}/'
object_name = s3_folder + zip_name

zip_files(folder_path, zip_name)
upload_to_s3(zip_name, S3_BUCKET_NAME, object_name)
unsigned_url = get_s3_url(S3_BUCKET_NAME, object_name)
print(f"Unsigned URL: {unsigned_url}")
