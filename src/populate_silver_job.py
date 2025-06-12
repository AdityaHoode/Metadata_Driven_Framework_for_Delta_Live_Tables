# Databricks notebook source
# MAGIC %md
# MAGIC # Note
# MAGIC - Sample for populating the Silver Metadata Table.
# MAGIC - This Notebook is designed to run in a Databricks workflow when deployed through CI/CD. 

# COMMAND ----------

dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Schema',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='')

# COMMAND ----------

target_catalog =dbutils.widgets.get("Target_Catalog")

# COMMAND ----------

target_schema = dbutils.widgets.get("Target_Schema")

# COMMAND ----------

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

# COMMAND ----------

meta_schema = dbutils.widgets.get("Metadata_Schema")

# COMMAND ----------

env =dbutils.widgets.get("env")

# COMMAND ----------

from dlt_helpers.populate_md import populate_silver
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------

# DBTITLE 1,SCD 2

dataFlowId = '200-Nominations-SCD2' # Unique ID for the dataflow -- PK
dataFlowGroup = "S1" # Dataflow group ID -- PK
sourceFormat = "delta" # Reading from Bronze Layer Delta Table
sourceDetails = {"database" : f"mdf4{env}.bronze","table": "cmf_nominations_scd2"}  #Source Table Details
readerConfigOptions = None 
targetFormat = 'delta' # Writing to Silver Layer Delta Table
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"cmf_nominations_scd2"}
tableProperties = None
## Select Expression to be used for eliminating columns, change column names, add new columns, change data types etc. here we are also generating a surrogate key using MD5 hash of all columns except the ones mentioned in the except clause and converting it to decimal(32,0)
selectExp = ["MD5(CONCAT_WS('', id, client, status)) AS surrogate_key", "* EXCEPT (_rescued_data,processing_time)","current_timestamp() as processing_time"] #(REQUIRED)
whereClause = None
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
cdcApplyChanges = None # Example:  #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}' Documentation: https://docs.databricks.com/en/delta-live-tables/cdc.html
materiazedView = None
## IF this field is populated, the dataflow will be treated as a Materialized View 
# and the selectExp will be used to create the view.
# The sourceDetails will be IGNORED in favor of the selectExp.
# DataQualityExpectations will be IGNORED.
# cdcApplyChanges will be IGNORED.
# whereClause will be IGNORED.
# SelectExp will be IGNORED.
## Example of how to create a SQL Statement
## For Materialized Views.
# """
# SELECT
#     last_updated,
#     ttl,
#     version,
#     station.station_id,
#     ebike.battery_charge_percentage,
#     ebike.displayed_number,
#     ebike.docking_capability,
#     ebike.is_lbs_internal_rideable,
#     ebike.make_and_model,
#     ebike.range_estimate.conservative_range_miles AS conservative_range_miles,
#     ebike.range_estimate.estimated_range_miles AS estimated_range_miles,
#     ebike.rideable_id
# FROM
#     mehdidatalake_catalog.retail_cdc.ebikes_at_station_bronze_dlt_meta
# LATERAL VIEW explode(data.stations) AS station
# LATERAL VIEW explode(station.ebikes) AS ebike
# """
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.silver_dataflowspec_table" # type: ignore

## Populate silver function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.
populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns,liquidClusteringColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)

# COMMAND ----------

# DBTITLE 1,SCD 1

dataFlowId = '200-Nominations-SCD1' # Unique ID for the dataflow -- PK
dataFlowGroup = "S1" # Dataflow group ID -- PK
sourceFormat = "delta" # Reading from Bronze Layer Delta Table
sourceDetails = {"database" : f"mdf4{env}.bronze","table": "cmf_nominations_scd1"}  #Source Table Details
readerConfigOptions = None 
targetFormat = 'delta' # Writing to Silver Layer Delta Table
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"cmf_nominations_scd1"}
tableProperties = None
## Select Expression to be used for eliminating columns, change column names, add new columns, change data types etc. here we are also generating a surrogate key using MD5 hash of all columns except the ones mentioned in the except clause and converting it to decimal(32,0)
selectExp = ["MD5(CONCAT_WS('', id, client, status)) AS surrogate_key", "* EXCEPT (_rescued_data,processing_time)","current_timestamp() as processing_time"] #(REQUIRED)
whereClause = None
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
cdcApplyChanges = None # Example:  #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}' Documentation: https://docs.databricks.com/en/delta-live-tables/cdc.html
materiazedView = None
## IF this field is populated, the dataflow will be treated as a Materialized View 
# and the selectExp will be used to create the view.
# The sourceDetails will be IGNORED in favor of the selectExp.
# DataQualityExpectations will be IGNORED.
# cdcApplyChanges will be IGNORED.
# whereClause will be IGNORED.
# SelectExp will be IGNORED.
## Example of how to create a SQL Statement
## For Materialized Views.
# """
# SELECT
#     last_updated,
#     ttl,
#     version,
#     station.station_id,
#     ebike.battery_charge_percentage,
#     ebike.displayed_number,
#     ebike.docking_capability,
#     ebike.is_lbs_internal_rideable,
#     ebike.make_and_model,
#     ebike.range_estimate.conservative_range_miles AS conservative_range_miles,
#     ebike.range_estimate.estimated_range_miles AS estimated_range_miles,
#     ebike.rideable_id
# FROM
#     mehdidatalake_catalog.retail_cdc.ebikes_at_station_bronze_dlt_meta
# LATERAL VIEW explode(data.stations) AS station
# LATERAL VIEW explode(station.ebikes) AS ebike
# """
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.silver_dataflowspec_table" # type: ignore

## Populate silver function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.
populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns,liquidClusteringColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)

# COMMAND ----------

# DBTITLE 1,MV

dataFlowId = '200-Nominations-MV' # Unique ID for the dataflow -- PK
dataFlowGroup = "S1" # Dataflow group ID -- PK
sourceFormat = "delta" # Reading from Bronze Layer Delta Table
sourceDetails = {"database" : f"mdf4{env}.bronze","table": "cmf_nominations_scd1"}  #Source Table Details
readerConfigOptions = None 
targetFormat = 'delta' # Writing to Silver Layer Delta Table
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"cmf_nominations_mv"}
tableProperties = None
## Select Expression to be used for eliminating columns, change column names, add new columns, change data types etc. here we are also generating a surrogate key using MD5 hash of all columns except the ones mentioned in the except clause and converting it to decimal(32,0)
selectExp = ["MD5(CONCAT_WS('', id, client, status)) AS surrogate_key", "* EXCEPT (_rescued_data,processing_time)","current_timestamp() as processing_time"] #(REQUIRED)
whereClause = None
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
cdcApplyChanges = None # Example:  #'{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["file_path","processing_time"], "except_column_list": ["operation"], "keys": ["customer_id"], "scd_type": "2", "sequence_by": "operation_date"}' Documentation: https://docs.databricks.com/en/delta-live-tables/cdc.html
materiazedView = """
select 
b.* 
from 
table_changes('mdf4.bronze.cmf_nominations_scd1', 2) a 
inner join table_changes('mdf4.bronze.cmf_nominations_scd2', 2) b 
on 
a.id = b.id
"""
## IF this field is populated, the dataflow will be treated as a Materialized View 
# and the selectExp will be used to create the view.
# The sourceDetails will be IGNORED in favor of the selectExp.
# DataQualityExpectations will be IGNORED.
# cdcApplyChanges will be IGNORED.
# whereClause will be IGNORED.
# SelectExp will be IGNORED.
## Example of how to create a SQL Statement
## For Materialized Views.
# """
# SELECT
#     last_updated,
#     ttl,
#     version,
#     station.station_id,
#     ebike.battery_charge_percentage,
#     ebike.displayed_number,
#     ebike.docking_capability,
#     ebike.is_lbs_internal_rideable,
#     ebike.make_and_model,
#     ebike.range_estimate.conservative_range_miles AS conservative_range_miles,
#     ebike.range_estimate.estimated_range_miles AS estimated_range_miles,
#     ebike.rideable_id
# FROM
#     mehdidatalake_catalog.retail_cdc.ebikes_at_station_bronze_dlt_meta
# LATERAL VIEW explode(data.stations) AS station
# LATERAL VIEW explode(station.ebikes) AS ebike
# """
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy = spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.silver_dataflowspec_table" # type: ignore

## Populate silver function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.
populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties,selectExp,whereClause,partitionColumns,liquidClusteringColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)
