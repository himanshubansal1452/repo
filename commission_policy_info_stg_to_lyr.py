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
AmazonRedshift_node1657966204972 = glueContext.create_dynamic_frame.from_catalog(
    database="eds_db",
    redshift_tmp_dir=args["TempDir"],
    table_name="dev_eds_stg_commission_policy_info",
    transformation_ctx="AmazonRedshift_node1657966204972"
)

# Script generated for node Apply Mapping
ApplyMapping_node1657966215371 = ApplyMapping.apply(
    frame=AmazonRedshift_node1657966204972,
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
        ("agent1_split_per_in_policy","string", "agent1_split_per_in_policy","string"),
        ("agent_number_2", "string", "agent_number_2", "string"),
        ("agent2_split_per_in_policy","string", "agent2_split_per_in_policy","string")
    ],
    transformation_ctx="ApplyMapping_node1657966215371"
)

# Script generated for node Amazon Redshift
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = ApplyMapping_node1657966215371, 
    catalog_connection="redshiftconn",
    connection_options = {"preactions":"drop table if exists eds_lyr.lyr_commission_policy_info_bkp;create table eds_lyr.lyr_commission_policy_info_bkp as select * from eds_lyr.lyr_commission_policy_info where 1=2;","dbtable":"eds_lyr.lyr_commission_policy_info_bkp", "database":"dev", "postactions":"delete from eds_lyr.lyr_commission_policy_info using eds_lyr.lyr_commission_policy_info_bkp where eds_lyr.lyr_commission_policy_info_bkp.policy_number = eds_lyr.lyr_commission_policy_info.policy_number;insert into eds_lyr.lyr_commission_policy_info select * from eds_lyr.lyr_commission_policy_info_bkp; drop table eds_lyr.lyr_commission_policy_info_bkp;update eds_stg.file_control set status = 'L' where filename like 'CommissionPolicyInfo%' and CYCLE_DATE in(select to_char(current_date,'YYYYMMDD'))"},
    redshift_tmp_dir = 's3://gluelogscelsior',
    transformation_ctx = "datasink5"
    )

CommissionPolicyInfo = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1657966215371,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://tpafiledir-archieve/", "partitionKeys": []},
    transformation_ctx="CommissionPolicyInfo"
)

job.commit()
