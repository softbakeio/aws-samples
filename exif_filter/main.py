import os
import boto3
from botocore.exceptions import NoCredentialsError
from requests.adapters import HTTPAdapter, Retry
from PIL import Image
from io import BytesIO
import requests
from typing import Any, Dict, List

# Configure AWS SDK using environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Create an S3 service object
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

def insecure_requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # session.verify = False  # Optionally disable SSL verification
    return session

def remove_exif_and_upload(url, key):
    response = requests.get(url)
    original_image = Image.open(BytesIO(response.content))

    # Create a new image without EXIF data
    data = list(original_image.getdata())
    image_no_exif = Image.new(original_image.mode, original_image.size)
    image_no_exif.putdata(data)

    # Save the new image to a BytesIO object
    buffer = BytesIO()
    image_no_exif.save(buffer, format=original_image.format)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket_name, key)

    print(f"Uploaded cleaned image with original file extension: {key}")

def process_images(data: Dict[str, str]):
    for url, key in data.items():
        remove_exif_and_upload(url, key)

def get_image_urls() -> Dict[str, str]:
    marker = ""
    image_urls = dict()

    while True:
        params = {
            'Bucket': bucket_name,
            'Marker': marker
        }
        try:
            data = s3.list_objects(**params)
            contents = data.get('Contents', [])
            if not contents:  # Check if the contents list is empty
                break  # Break the loop if no more items are available

            for item in contents:
                if item['Key'].lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': item['Key']})
                    # print("url is {} and key is {}".format(url, item['Key']))
                    image_urls[url] = item['Key']

            marker = contents[-1]['Key']  # Update the marker with the last key of the current page
        except NoCredentialsError as e:
            print("Error in processing S3 objects:", str(e))
            break
    
    return image_urls

if __name__ == "__main__":
    data = get_image_urls()
    process_images(data)
    print('Processing completed.')