# Databricks notebook source
# MAGIC %md
# MAGIC # Note
# MAGIC - Sample for populating the Bronze Metadata Table.
# MAGIC - This Notebook is designed to run in a Databricks workflow when deployed through CI/CD. 
# MAGIC

# COMMAND ----------

dbutils.widgets.text('env',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Target_Schema',defaultValue='edw_bluebikes_ebikes_bronze')

# COMMAND ----------

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

# COMMAND ----------

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')

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

from dlt_helpers.populate_md import populate_bronze
import datetime
from pyspark.sql.functions import current_user

# COMMAND ----------

# DBTITLE 1,SCD 2
dataFlowId = '100-Nominations-SCD2' # Dataflow ID -PK
dataFlowGroup = "B1" # Dataflow Group -PK ## ALL the dataflows with the same group are executed in the same DLT pipeline
sourceFormat = "cloudFiles" # Format of the source, for files on Cloud storage use "cloudFiles"
sourceDetails = {
        "path":f"abfss://mdf4@stgacct14022025.dfs.core.windows.net/Nominations", # For Auto Loader directory listing mode
        # "path":f"abfss://mdf5@stgacct14022025.dfs.core.windows.net/2025/*/*/Nominations/", # For Auto Loader file notification mode
        "source_database":"cmf_nominations_scd2",
        "source_table":"cmf_nominations_scd2"
    } #Source Details, If "cloudFiles" the database and table are ignore and are only stored in MD table for reference
highWaterMark = {"contract_id":"100","contract_version":"1.000","contract_major_version":"1","watermark_column": "processing_time"} # High Water Mark for the dataflow, if not provided, It will be populating as NULL {"contract_id":"abc123-456-cfd","contract_version":"1.000","contract_major_version":"1","watermark_column": "operation_date"}
readerConfigOptions ={
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFile.readerCaseSensitive": "false",
        "cloudFiles.useNotifications": "false", # Auto Loader mode 1: Legacy file notification mode
        # "cloudFiles.useManagedFileEvents": "true", # Auto Loader mode 2: File events (Recommended)
        "header": "true",
        "inferSchema": "true"
    } ## Reader Configuration - Refer to DB documentation https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html
cloudFileNotificationsConfig = None ## If using Auto Loader file notification mode 1
# {
#         "cloudFiles.subscriptionId": "az_subscription_id",
#         "cloudFiles.tenantId": "adls_tenant_id_key_name",
#         "cloudFiles.resourceGroup": "az_resource_group",
#         "cloudFiles.clientId": "adls_client_id_key_name",
#         "cloudFiles.clientSecret": "adls_secret_key_name",
#         "configJsonFilePath": "dbfs:/FileStore/Mehdi_Modarressi/config/config-2.json",
#         "kvSecretScope": "key_vault_name"
#     }
schema = None # '{"fields":[{"metadata":{},"name":"userId","nullable":true,"type":"integer"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"},{"metadata":{},"name":"operation","nullable":true,"type":"string"},{"metadata":{},"name":"sequenceNum","nullable":true,"type":"integer"},{"metadata":{},"name":"_rescued_data","nullable":true,"type":"string"}],"type":"struct"}' # If inpute schema is defined, rather than the infer schema option
targetFormat = 'delta' ## Target format, almost always "delta"
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"cmf_nominations_scd2"}
tableProperties = {"delta.enableChangeDataFeed": "true"}
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
# cdcApplyChanges = '{"apply_as_deletes": "operation_type = \'D\'", "track_history_except_column_list": ["operation_type", "operation_timestamp", "position"], "keys": ["id"], "scd_type": "2", "sequence_by": "operation_timestamp"}' # '{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["operation", "sequenceNum"], "except_column_list": ["operation", "sequenceNum"], "keys": ["userId"], "scd_type": "2", "sequence_by": "sequenceNum"}'
cdcApplyChanges = '{"apply_as_deletes": "operation_type = \'D\'", "track_history_except_column_list": ["operation_type", "operation_timestamp", "position", "_rescued_data", "file_path", "processing_time"], "keys": ["id"], "scd_type": "2", "sequence_by": ["operation_timestamp", "position"]}'
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy =  spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.bronze_dataflowspec_table" # Bronze Metadata Table

## Populate Bronze function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.

populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,highWaterMark,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,liquidClusteringColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)

# COMMAND ----------

# DBTITLE 1,SCD 1
dataFlowId = '101-Nominations-SCD1' # Dataflow ID -PK
dataFlowGroup = "B1" # Dataflow Group -PK ## ALL the dataflows with the same group are executed in the same DLT pipeline
sourceFormat = "cloudFiles" # Format of the source, for files on Cloud storage use "cloudFiles"
sourceDetails = {
        "path":f"abfss://mdf4@stgacct14022025.dfs.core.windows.net/Nominations", # For Auto Loader directory listing mode
        # "path":f"abfss://mdf5@stgacct14022025.dfs.core.windows.net/2025/*/*/Nominations/", # For Auto Loader file notification mode
        "source_database":"cmf_nominations_scd1",
        "source_table":"cmf_nominations_scd1"
    } #Source Details, If "cloudFiles" the database and table are ignore and are only stored in MD table for reference
highWaterMark = {"contract_id":"200","contract_version":"1.000","contract_major_version":"1","watermark_column": "processing_time"} # High Water Mark for the dataflow, if not provided, It will be populating as NULL {"contract_id":"abc123-456-cfd","contract_version":"1.000","contract_major_version":"1","watermark_column": "operation_date"}
readerConfigOptions ={
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFile.readerCaseSensitive": "false",
        "cloudFiles.useNotifications": "false", # Auto Loader mode 1: Legacy file notification mode
        # "cloudFiles.useManagedFileEvents": "true", # Auto Loader mode 2: File events (Recommended)
        "header": "true",
        "inferSchema": "true"
    } ## Reader Configuration - Refer to DB documentation https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html
cloudFileNotificationsConfig = None ## If using CloudFilesNotification Mode
# {
#         "cloudFiles.subscriptionId": "az_subscription_id",
#         "cloudFiles.tenantId": "adls_tenant_id_key_name",
#         "cloudFiles.resourceGroup": "az_resource_group",
#         "cloudFiles.clientId": "adls_client_id_key_name",
#         "cloudFiles.clientSecret": "adls_secret_key_name",
#         "configJsonFilePath": "dbfs:/FileStore/Mehdi_Modarressi/config/config-2.json",
#         "kvSecretScope": "key_vault_name"
#     }
schema = None # '{"fields":[{"metadata":{},"name":"userId","nullable":true,"type":"integer"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"},{"metadata":{},"name":"operation","nullable":true,"type":"string"},{"metadata":{},"name":"sequenceNum","nullable":true,"type":"integer"},{"metadata":{},"name":"_rescued_data","nullable":true,"type":"string"}],"type":"struct"}' # If inpute schema is defined, rather than the infer schema option
targetFormat = 'delta' ## Target format, almost always "delta"
targetDetails = {"database":f"{target_catalog}{env}.{target_schema}","table":"cmf_nominations_scd1"}
tableProperties = {"delta.enableChangeDataFeed": "true"}
partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
cdcApplyChanges = None # '{"apply_as_deletes": "operation = \'D\'", "track_history_except_column_list": ["operation_type", "operation_timestamp", "position"], "keys": ["id"], "scd_type": "2", "sequence_by": "operation_timestamp"}' # '{"apply_as_deletes": "operation = \'DELETE\'","track_history_except_column_list": ["operation", "sequenceNum"], "except_column_list": ["operation", "sequenceNum"], "keys": ["userId"], "scd_type": "2", "sequence_by": "sequenceNum"}'
dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
quarantineTargetDetails = None
quarantineTableProperties = None
createDate = datetime.datetime.now()
updateDate = datetime.datetime.now()
createdBy =  spark.range(1).select(current_user()).head()[0]
updatedBy = spark.range(1).select(current_user()).head()[0]
BRONZE_MD_TABLE = f"{meta_catalog}{env}.{meta_schema}.bronze_dataflowspec_table" # Bronze Metadata Table

## Populate Bronze function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.

populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,highWaterMark,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,liquidClusteringColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)
