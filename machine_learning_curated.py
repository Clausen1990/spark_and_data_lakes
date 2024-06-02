import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1716830747143 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1716830747143")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716830745463 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1716830745463")

# Script generated for node Join Step Trainer and Accelerometer
SqlQuery1423 = '''
select * from step_trainer_trusted 
join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
JoinStepTrainerandAccelerometer_node1716830799559 = sparkSqlQuery(glueContext, query = SqlQuery1423, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1716830747143, "accelerometer_trusted":AccelerometerTrusted_node1716830745463}, transformation_ctx = "JoinStepTrainerandAccelerometer_node1716830799559")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1716832273519 = glueContext.getSink(path="s3://udacity-glue-spark/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1716832273519")
MachineLearningCurated_node1716832273519.setCatalogInfo(catalogDatabase="udacity-spark",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1716832273519.setFormat("json")
MachineLearningCurated_node1716832273519.writeFrame(JoinStepTrainerandAccelerometer_node1716830799559)
job.commit()