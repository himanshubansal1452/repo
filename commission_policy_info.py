import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="eds_db",
    table_name="commissionpolicyinfo_csv",
    transformation_ctx="S3bucket_node1",
)

df = S3bucket_node1.toDF()
df=df.withColumn("policy_effective_date", to_date(col("policy_effective_date"),"dd/MM/yyyy"))
df=df.withColumn("transaction_date", to_date(col("transaction_date"),"dd/MM/yyyy"))
df=df.withColumn("annual_premium", col("annual_premium").cast('decimal(7,2)'))
df=df.withColumn("face_amount", col("face_amount").cast('decimal(9,2)'))
df=df.withColumn("insured_age", col("insured_age").cast('string'))
editedFrame = DynamicFrame.fromDF(df,glueContext,"editedFrame")

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=editedFrame,
    mappings=[
        ("policy_number", "string", "policy_number", "string"),
        ("company_code", "string", "company_code", "string"),
        ("commission_company_code", "string", "commission_company_code", "string"),
        ("transaction_type", "string", "transaction_type", "string"),
        ("product_name_plan_code", "string", "product_name_plan_code", "string"),
        ("policy_effective_date", "date", "policy_effective_date", "date"),
        ("insured_name", "string", "insured_name", "string"),
        ("insured_age", "string", "insured_age", "string"),
        ("insured_gender", "string", "insured_gender", "string"),
        ("transaction_date", "date", "transaction_date", "date"),
        ("annual_premium", "decimal(7,2)", "annual_premium", "decimal(7,2)"),
        ("face_amount", "decimal(9,2)", "face_amount", "decimal(9,2)"),
        ("policy_issue_state", "string", "policy_issue_state", "string"),
        ("qualification_indicator", "string", "qualification_indicator", "string"),
        ("premium_payment_method", "string", "premium_payment_method", "string"),
        ("agent_number_1", "string", "agent_number_1", "string"),
        ("agent1_split__in_policy", "string", "agent1_split_per_in_policy", "string"),
        ("agent_number_2", "string", "agent_number_2", "string"),
        ("agent2_split__in_policy", "string", "agent2_split_per_in_policy", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
datasink5  = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=ApplyMapping_node2,
    catalog_connection="redshiftconn",
    connection_options = {"preactions":"truncate table eds_stg.commission_policy_info;","dbtable":"eds_stg.commission_policy_info", "database":"dev", "postactions":"update eds_stg.file_control set status = 'S' where filename like 'CommissionPolicyInfo%' and CYCLE_DATE in(select to_char(current_date,'YYYYMMDD'))"},
    redshift_tmp_dir='s3://gluelogscelsior',
    transformation_ctx = "datasink5")
    
job.commit()
