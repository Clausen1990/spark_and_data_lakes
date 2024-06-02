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

# Script generated for node Cutomer Landing
CutomerLanding_node1716810031618 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/customer/landing/"], "recurse": True}, transformation_ctx="CutomerLanding_node1716810031618")

# Script generated for node PrivacyFilter
SqlQuery1376 = '''
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate != 0;
'''
PrivacyFilter_node1716816384033 = sparkSqlQuery(glueContext, query = SqlQuery1376, mapping = {"customer_landing":CutomerLanding_node1716810031618}, transformation_ctx = "PrivacyFilter_node1716816384033")

# Script generated for node Customer Trusted
CustomerTrusted_node1716816505603 = glueContext.getSink(path="s3://udacity-glue-spark/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1716816505603")
CustomerTrusted_node1716816505603.setCatalogInfo(catalogDatabase="udacity-spark",catalogTableName="customer_trusted")
CustomerTrusted_node1716816505603.setFormat("json")
CustomerTrusted_node1716816505603.writeFrame(PrivacyFilter_node1716816384033)
job.commit()