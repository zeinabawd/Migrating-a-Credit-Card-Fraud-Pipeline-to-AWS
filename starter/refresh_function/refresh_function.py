import boto3
import os
from datetime import datetime
from pyspark.sql import SparkSession
from fraud_detection_pipeline.fraud_detector_model_trainer import MyTransform

s3 = boto3.client('s3')
bucket_name = 'fraud-detection-project-bucket'

def lambda_handler(event, context):
    try:
        # Get the uploaded file info from the event
        for record in event['Records']:
            key = record['s3']['object']['key']
            print(f"New data detected: {key}")

            # Read the new data from S3
            s3_uri = f"s3://{bucket_name}/{key}"
            spark = SparkSession.builder.getOrCreate()
            new_data = spark.read.csv(s3_uri, header=True, inferSchema=True)

            # Train the model with the new data
            MyTransform(new_data)

            # Archive the processed file in S3
            archive_key = f"archive/{os.path.basename(key)}_{datetime.now().strftime('%Y%m%d')}"
            s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': key}, Key=archive_key)
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"File archived as: {archive_key}")

    except Exception as e:
        print(f"Error: {e}")
