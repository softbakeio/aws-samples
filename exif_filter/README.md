# EXIF image filter
 
EXIF image filter which read images from S3 and remove EXIF properties and then upload back

## Getting started

### First setup environment variables

> export S3_BUCKET_NAME=dev.cd3.incdatagate.cz &&
> export AWS_SECRET_ACCESS_KEY=<secret> && 
> export AWS_ACCESS_KEY_ID=<access-key-id> &&
> export AWS_REGION=eu-central-1

### Install depedencies

> pip install -r requirements.txt

### Run script

> python main.py