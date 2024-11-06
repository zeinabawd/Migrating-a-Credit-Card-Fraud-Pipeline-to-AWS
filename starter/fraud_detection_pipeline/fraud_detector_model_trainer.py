from awsglue.transforms import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
import boto3

# Initialize GlueContext for AWS Glue
glueContext = GlueContext(SparkSession.builder.getOrCreate())
s3 = boto3.client('s3')

def MyTransform(dynamic_frame):
    try:
        # Convert DynamicFrame to DataFrame
        df = dynamic_frame.toDF()

        # Convert numeric columns to DoubleType
        numeric_columns = [col for col in df.columns if col != 'Class']
        for column in numeric_columns:
            df = df.withColumn(column, col(column).cast(DoubleType()))

        # Define feature transformation steps
        assembler = VectorAssembler(inputCols=numeric_columns, outputCol="numericFeatures")
        scaler = StandardScaler(inputCol="numericFeatures", outputCol="scaledFeatures", withStd=True, withMean=True)
        finalAssembler = VectorAssembler(inputCols=["scaledFeatures"], outputCol="features")

        # Handle class imbalance
        class_counts = df.groupBy("Class").count().collect()
        total_count = sum(row['count'] for row in class_counts)
        weight_dict = {row['Class']: total_count / row['count'] for row in class_counts}
        df = df.withColumn("weight", when(col("Class") == 0, weight_dict[0]).otherwise(weight_dict[1]))

        # Define the RandomForest model and pipeline
        indexer = StringIndexer(inputCol="Class", outputCol="label")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=10, weightCol="weight")
        pipeline = Pipeline(stages=[assembler, scaler, finalAssembler, indexer, rf])

        # Train the model
        model = pipeline.fit(df)

        # Save the model to S3
        model_path = "s3://fraud-detection-project-bucket/fraud_detection_model_latest"
        model.write().overwrite().save(model_path)
        print(f'Model saved to {model_path}')

        # Convert DataFrame back to DynamicFrame
        return DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    except Exception as e:
        print(f"Error: {e}")
