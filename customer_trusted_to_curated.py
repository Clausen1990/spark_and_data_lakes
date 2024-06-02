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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1716821691890 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometertrusted_node1716821691890")

# Script generated for node Customer trusted
Customertrusted_node1716821794514 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusted_node1716821794514")

# Script generated for node Customer Privacy Filter
SqlQuery1150 = '''
select * from customer_trusted 
join accelerometer_trusted on customer_trusted.email = accelerometer_trusted.user
'''
CustomerPrivacyFilter_node1716821830133 = sparkSqlQuery(glueContext, query = SqlQuery1150, mapping = {"customer_trusted":Customertrusted_node1716821794514, "accelerometer_trusted":Accelerometertrusted_node1716821691890}, transformation_ctx = "CustomerPrivacyFilter_node1716821830133")

# Script generated for node Drop Fields
DropFields_node1716822367994 = DropFields.apply(frame=CustomerPrivacyFilter_node1716821830133, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1716822367994")

# Script generated for node Drop Duplicates
DropDuplicates_node1716829122498 =  DynamicFrame.fromDF(DropFields_node1716822367994.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716829122498")

# Script generated for node Customer curated
Customercurated_node1716822393744 = glueContext.getSink(path="s3://udacity-glue-spark/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customercurated_node1716822393744")
Customercurated_node1716822393744.setCatalogInfo(catalogDatabase="udacity-spark",catalogTableName="customer_curated")
Customercurated_node1716822393744.setFormat("json")
Customercurated_node1716822393744.writeFrame(DropDuplicates_node1716829122498)
job.commit()