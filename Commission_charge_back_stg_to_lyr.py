import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.job import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon Redshift
AmazonRedshift_node1657963038600 = glueContext.create_dynamic_frame.from_catalog(
    database="eds_db",
    redshift_tmp_dir=args["TempDir"],
    table_name="dev_eds_stg_commission_charge_back",
    transformation_ctx="AmazonRedshift_node1657963038600",
)

# Script generated for node Apply Mapping
ApplyMapping_node1657963054955 = ApplyMapping.apply(
    frame=AmazonRedshift_node1657963038600,
    mappings=[
        ("policy_number", "string", "policy_number", "string"),
        ("company_code", "string", "company_code", "string"),
        ("policy_effective_date", "date", "policy_effective_date", "date"),
        ("insured_name", "string", "insured_name", "string"),
        ("insured_age", "string", "insured_age", "string"),
        ("insured_gender", "string", "insured_gender", "string"),
        ("face_amount", "decimal", "face_amount", "decimal"),
        ("fund_name1", "string", "fund_name1", "string"),
        ("fund1_per", "string", "fund1_per", "string"),
        ("fund_name2", "string", "fund_name2", "string"),
        ("fund2_per", "string", "fund2_per", "string")
    ],
    transformation_ctx="ApplyMapping_node1657963054955"
)

# Script generated for node Amazon Redshift
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = ApplyMapping_node1657963054955, 
    catalog_connection="redshiftconn",
    connection_options = {"preactions":"drop table if exists eds_lyr.lyr_commission_charge_back_bkp;create table eds_lyr.lyr_commission_charge_back_bkp as select * from eds_lyr.lyr_commission_charge_back where 1=2;","dbtable":"eds_lyr.lyr_commission_charge_back_bkp", "database":"dev", "postactions":"delete from eds_lyr.lyr_commission_charge_back using eds_lyr.lyr_commission_charge_back_bkp where eds_lyr.lyr_commission_charge_back_bkp.policy_number = eds_lyr.lyr_commission_charge_back.policy_number;insert into eds_lyr.lyr_commission_charge_back select * from eds_lyr.lyr_commission_charge_back_bkp; drop table eds_lyr.lyr_commission_charge_back_bkp;update eds_stg.file_control set status = 'L' where filename like 'CommissionChargeback%' and CYCLE_DATE in(select to_char(current_date,'YYYYMMDD'))"},
    redshift_tmp_dir = 's3://gluelogscelsior',
    transformation_ctx = "datasink5"
    )

CommissionChargeback = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1657963054955,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://tpafiledir-archieve/", "partitionKeys": []},
    transformation_ctx="CommissionChargeback"
)

job.commit()
