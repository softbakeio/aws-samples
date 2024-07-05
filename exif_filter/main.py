import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import TypedDict

import boto3
import requests
from PIL import Image

# Configure AWS SDK using environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('S3_BUCKET_NAME')


# Create an S3 service object
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

S3ImageObject = TypedDict('S3ImageObject', {'image_url': str, 'image_path': str})


def exponential_backoff_delay_interval(initial_delay_in_seconds=2, attempts=3):
    delay = initial_delay_in_seconds
    yield delay
    for _ in range(attempts):
        delay *= 2
        yield delay


processed_images = 0


def remove_exif_and_upload(image_object: S3ImageObject):
    global processed_images
    image_response = requests.get(image_object["image_url"])

    # create backup on local FS
    # path = '/'.join(image_object["image_path"].split('/')[:-1])
    # os.makedirs(f'backup/{path}', exist_ok=True)
    # with open(f'backup/{image_object["image_path"]}', 'wb') as f:
    #     f.write(image_response.content)

    original_image_bytes = BytesIO(image_response.content)
    original_image = Image.open(original_image_bytes)

    # Create a new image without EXIF data
    data = list(original_image.getdata())
    image_no_exif = Image.new(original_image.mode, original_image.size)
    image_no_exif.putdata(data)

    # Save the new image to a BytesIO object
    buffer = BytesIO()
    image_no_exif.save(buffer, format=original_image.format)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket_name, image_object["image_path"])
    processed_images += 1
    print(f'processed images: {processed_images}')
    print(f'Uploaded cleaned image with original file extension: {image_object["image_path"]}')


class ProcessImageFunction:
    def __init__(self, image: S3ImageObject):
        self.image: S3ImageObject = image

    def __call__(self):
        remove_exif_and_upload(self.image)


def invoke_with_retry(function: ProcessImageFunction):
    attempts = 3
    for wait_interval_in_seconds in exponential_backoff_delay_interval(attempts=attempts):
        try:
            function()
            print(f'function {function}({function.image["image_path"]}) succeeded')
            return
        except Exception as e:
            print(f'ERROR: function {function}({function.image["image_path"]}) failed ({e}), retrying...')
            time.sleep(wait_interval_in_seconds)
    print(f'ERROR: function {function}({function.image}) failed after {attempts} attemps')


def process_images(images: list[S3ImageObject], max_workers: int):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(invoke_with_retry, ProcessImageFunction(image)) for image in images]
        for future in futures:
            future.result()


def get_images() -> list[S3ImageObject]:
    results = []
    paginator = s3.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket_name}
    page_iterator = paginator.paginate(**operation_parameters)
    for page_index, page in enumerate(page_iterator):
        print(f'procesing page: {page_index}')
        contents = page.get('Contents', [])
        for item in contents:
            if item['Key'].lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                print(f'getting url for image: {item["Key"]}')
                image_url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': item['Key']})
                results.append({
                    "image_url": image_url,
                    "image_path": item['Key']
                })
    return results


if __name__ == "__main__":
    images: list[S3ImageObject] = get_images()
    number_of_images_to_process = len(images)
    print(f'number_of_images_to_process: {number_of_images_to_process}')
    process_images(images, max_workers=8)
    print('Processing completed.')