from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import boto3

class InputData(BaseModel):
    features: list[float]

app = FastAPI()
spark = SparkSession.builder.appName("FraudDetectionAPI").getOrCreate()
s3 = boto3.client('s3')

model = None

@app.on_event("startup")
async def load_model():
    global model
    # Download the model from S3 to local storage
    bucket_name = 'fraud-detection-project-bucket'
    model_key = 'fraud_detection_model_latest'
    local_path = '/tmp/fraud_detection_model_latest'
    s3.download_file(bucket_name, model_key, local_path)

    # Load the model
    model = PipelineModel.load(local_path)

@app.post("/predict/")
def predict(data: InputData):
    columns = ['Time', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 
               'V10', 'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 
               'V19', 'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 
               'V28', 'Amount']
    df = spark.createDataFrame([data.features], columns)
    predictions = model.transform(df)
    prediction = predictions.select("prediction").collect()[0][0]
    return {'prediction': prediction}
