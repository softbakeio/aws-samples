import os
import boto3
from botocore.exceptions import NoCredentialsError
import requests
from PIL import Image
from io import BytesIO
# from exif import Image as ExifImage
from retry import retry

# Configure AWS SDK
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Create an S3 service object
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

# # Function to get the EXIF data from an image URL
# def print_exif(url):
#     response = requests.get(url)
#     img = ExifImage(BytesIO(response.content))
#     if img.has_exif:
#         print(img.list_all())
#     else:
#         print("No EXIF data found.")

# Function to remove EXIF data and upload to S3
@retry(tries=3, delay=1)
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

# Function to process all image files in the bucket
def process_images(marker=""):
    params = {
        'Bucket': bucket_name,
        'StartAfter': marker
        # 'Prefix': 'images/1ed577fd-7a8b-68ec-ab1e-993b39798311'
    }

    try:
        data = s3.list_objects_v2(**params)
        for item in data.get('Contents', []):
            if item['Key'].lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                url = s3.generate_presigned_url('get_object',
                                                Params={'Bucket': bucket_name, 'Key': item['Key']},
                                                ExpiresIn=60)
                # print_exif(url)
                remove_exif_and_upload(url, item['Key'])
                # print_exif(url)
        if data.get('IsTruncated'):
            process_images(data.get('NextContinuationToken'))
    except NoCredentialsError as e:
        print("Error in processing S3 objects:", str(e))

if __name__ == "__main__":
    process_images()
    print('Processing completed.')

