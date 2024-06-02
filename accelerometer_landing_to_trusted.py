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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1716821691890 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1716821691890")

# Script generated for node Customer trusted
Customertrusted_node1716821794514 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-glue-spark/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusted_node1716821794514")

# Script generated for node Customer Privacy Filter
SqlQuery1104 = '''
select * from customer_trusted 
join accelerometer_landing on customer_trusted.email = accelerometer_landing.user
'''
CustomerPrivacyFilter_node1716821830133 = sparkSqlQuery(glueContext, query = SqlQuery1104, mapping = {"customer_trusted":Customertrusted_node1716821794514, "accelerometer_landing":Accelerometerlanding_node1716821691890}, transformation_ctx = "CustomerPrivacyFilter_node1716821830133")

# Script generated for node Drop Fields
DropFields_node1716822367994 = DropFields.apply(frame=CustomerPrivacyFilter_node1716821830133, paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1716822367994")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1716822393744 = glueContext.getSink(path="s3://udacity-glue-spark/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1716822393744")
Accelerometertrusted_node1716822393744.setCatalogInfo(catalogDatabase="udacity-spark",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1716822393744.setFormat("json")
Accelerometertrusted_node1716822393744.writeFrame(DropFields_node1716822367994)
job.commit()