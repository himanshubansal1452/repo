
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.context import *
from pyspark.context import *

##Pushing to Git
##Himanshu Added One function
##Pulling to Git

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "eds_db", table_name = "investment_csv_4aae79bcc92cee1fcf79b01fb12622c8", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "eds_db", table_name = "investment_csv_4aae79bcc92cee1fcf79b01fb12622c8", transformation_ctx = "datasource0")

df = datasource0.toDF()
df=df.withColumn("equity_buy_date", to_date(col("equity_buy_date"),"dd/MM/yyyy"))
df=df.withColumn("next_buy_date", to_date(col("next_buy_date"),"dd/MM/yyyy"))
df=df.withColumn("account_value", col("account_value").cast('decimal(9,2)'))
df=df.withColumn("surrender_value_for_the_fund", col("surrender_value_for_the_fund").cast('decimal(9,2)'))
df=df.withColumn("penalty_charge", col("penalty_charge").cast('decimal(9,2)'))
editedFrame = DynamicFrame.fromDF(df,glueContext,"editedFrame")

applymapping1 = ApplyMapping.apply(frame = editedFrame, mappings = [("policy_number", "string", "policy_number", "string"), ("company_code", "string", "company_code", "string"), ("equity_buy_date", "date", "equity_buy_date", "date"), ("next_buy_date", "date", "next_buy_date", "date"), ("fund_name", "string", "fund_name", "string"), ("account_value", "decimal(9,2)", "account_value", "decimal(9,2)"), ("surrender_value_for_the_fund", "decimal(9,2)", "surrender_value_for_the_fund", "decimal(9,2)"), ("penalty_charge", "decimal(8,2)", "penalty_charge", "decimal(8,2)"), ("fund_allocation_", "string", "fund_allocation", "string"), ("fundcode", "string", "fundcode", "string")], transformation_ctx = "applymapping1")

datasink5  = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=applymapping1,
    catalog_connection="redshiftconn",
    connection_options = {"preactions":"truncate table eds_stg.investment;","dbtable":"eds_stg.investment", "database":"dev", "postactions":"update eds_stg.file_control set status = 'S' where filename like 'Investment%' and CYCLE_DATE in(select to_char(current_date,'YYYYMMDD'))"},
    redshift_tmp_dir='s3://gluelogscelsior',
    transformation_ctx = "datasink5")
job.commit()
