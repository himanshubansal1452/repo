import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon Redshift
AmazonRedshift_node1657971425434 = glueContext.create_dynamic_frame.from_catalog(
    database="eds_db",
    redshift_tmp_dir=args["TempDir"],
    table_name="dev_eds_stg_commission_premium_info",
    transformation_ctx="AmazonRedshift_node1657971425434",
)

# Script generated for node Apply Mapping
ApplyMapping_node1657971449743 = ApplyMapping.apply(
    frame=AmazonRedshift_node1657971425434,
    mappings=[
        ("policy_number", "string", "policy_number", "string"),
        ("company_code", "string", "company_code", "string"),
        ("insured_name", "string", "insured_name", "string"),
        ("insured_age", "string", "insured_age", "string"),
        ("insured_gender", "string", "insured_gender", "string"),
        ("transaction_type", "string", "transaction_type", "string"),
        ("product_name_plan_code", "string", "product_name_plan_code", "string"),
        ("policy_effective_date", "date", "policy_effective_date", "date"),
        ("transaction_date", "date", "transaction_date", "date"),
        ("transaction_amount", "decimal", "transaction_amount", "decimal"),
        ("fund_name1", "string", "fund_name1", "string"),
        ("fund1_per", "string", "fund1_per", "string"),
        ("fund_name2", "string", "fund_name2", "string"),
        ("fund2_per", "string", "fund2_per", "string")
        ],
    transformation_ctx="ApplyMapping_node1657971449743"
)

# Script generated for node Amazon Redshift
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = ApplyMapping_node1657971449743, 
    catalog_connection="redshiftconn",
    connection_options = {"preactions":"drop table if exists eds_lyr.lyr_commission_premium_info_bkp;create table eds_lyr.lyr_commission_premium_info_bkp as select * from eds_lyr.lyr_commission_premium_info where 1=2;","dbtable":"eds_lyr.lyr_commission_premium_info_bkp", "database":"dev", "postactions":"delete from eds_lyr.lyr_commission_premium_info using eds_lyr.lyr_commission_premium_info_bkp where eds_lyr.lyr_commission_premium_info_bkp.policy_number = eds_lyr.lyr_commission_premium_info.policy_number;insert into eds_lyr.lyr_commission_premium_info select * from eds_lyr.lyr_commission_premium_info_bkp; drop table eds_lyr.lyr_commission_premium_info_bkp;update eds_stg.file_control set status = 'L' where filename like 'CommissionPremiumInfo%' and CYCLE_DATE in(select to_char(current_date,'YYYYMMDD'))"},
    redshift_tmp_dir = 's3://gluelogscelsior',
    transformation_ctx = "datasink5"
    )

CommissionPremiumInfo = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1657971449743,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://tpafiledir-archieve/", "partitionKeys": []},
    transformation_ctx="CommissionPremiumInfo"
)

job.commit()
