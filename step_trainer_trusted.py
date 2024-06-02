import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer Curated
CustomerCurated_node1716829468598 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1716829468598")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1716829573255 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1716829573255")

# Script generated for node Step Trainer Filter Privacy
SqlQuery1498 = '''
select * from customer_curated 
join step_trainer_landing 
on customer_curated.serialnumber = step_trainer_landing.serialnumber
'''
StepTrainerFilterPrivacy_node1716829617380 = sparkSqlQuery(glueContext, query = SqlQuery1498, mapping = {"customer_curated":CustomerCurated_node1716829468598, "step_trainer_landing":StepTrainerLanding_node1716829573255}, transformation_ctx = "StepTrainerFilterPrivacy_node1716829617380")

# Script generated for node Drop Fields
DropFields_node1716830039059 = DropFields.apply(frame=StepTrainerFilterPrivacy_node1716829617380, paths=["customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1716830039059")

# Script generated for node Drop Duplicates
DropDuplicates_node1716830116062 =  DynamicFrame.fromDF(DropFields_node1716830039059.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716830116062")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1716830122702 = glueContext.getSink(path="s3://udacity-glue-spark/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1716830122702")
StepTrainerTrusted_node1716830122702.setCatalogInfo(catalogDatabase="udacity-spark",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1716830122702.setFormat("json")
StepTrainerTrusted_node1716830122702.writeFrame(DropDuplicates_node1716830116062)
job.commit()