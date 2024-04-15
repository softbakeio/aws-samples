import * as AWS from 'aws-sdk';
// @ts-ignore
import { fetch } from "undici";
import { ExifParserFactory } from 'ts-exif-parser';

// Configure AWS SDK
AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION
});

// Create an S3 service object
const s3 = new AWS.S3();

// Bucket name
const bucketName = 'dev.cd1.incdatagate.cz'; // Replace with your bucket name

/**
 * Generic retry function
 * @param fn Function to execute that returns a Promise
 * @param retries Number of retries
 * @param delay Delay in milliseconds between retries
 */
async function retry<T>(fn: () => Promise<T>, retries: number = 3, delay: number = 1000): Promise<T> {
    try {
        return await fn();
    } catch (error: any) {
        if (retries > 1) {
            console.log(`Retrying after error: ${error.message}`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return retry(fn, retries - 1, delay);
        } else {
            throw error;
        }
    }
}

// Function to get the EXIF data from an image URL
async function printEXIF(url: string) {
    return retry(async () => {
        const response = await fetch(url);
        const buffer = await response.arrayBuffer();
        const parser = ExifParserFactory.create(buffer);
        const result = parser.parse();
        console.log(result);
    });
}

// Function to list all image files
async function processImages(marker?: string): Promise<void> {
    const params: AWS.S3.ListObjectsV2Request = {
        Bucket: bucketName,
        StartAfter: marker,
        // You can specify a prefix if you are looking for files under a specific directory
    };

    try {
        const data = await s3.listObjectsV2(params).promise();
        for (const item of data.Contents || []) {
            if (item.Key && item.Key.match(/\.(jpg|jpeg|png|gif|webp)$/i)) {
                console.log(item.Key);
                const url = s3.getSignedUrl('getObject', {
                    Bucket: bucketName,
                    Key: item.Key,
                    Expires: 60 // URL expires in 60 seconds
                });
                await printEXIF(url);
            }
        }


        if (data.IsTruncated && data.NextContinuationToken) {
            await processImages(data.NextContinuationToken);
        }
    } catch (err) {
        console.error("Error in listing S3 objects:", err);
    }
}

processImages().then(() => console.log('Listing completed.'));
