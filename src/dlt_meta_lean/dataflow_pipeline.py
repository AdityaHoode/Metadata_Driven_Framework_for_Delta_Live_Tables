"""DataflowPipeline provide generic DLT code using dataflowspec."""
import json
import logging
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, struct
from pyspark.sql.types import StructType, StructField
import inspect

from dlt_meta_lean.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
from dlt_meta_lean.pipeline_readers import PipelineReaders

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class DataflowPipeline:
    """This class uses dataflowSpec to launch DLT.

    Raises:
        Exception: "Dataflow not supported!"

    Returns:
        [type]: [description]
    """

    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine=None):
        """Initialize Constructor."""
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        if isinstance(dataflow_spec, BronzeDataflowSpec) or isinstance(dataflow_spec, SilverDataflowSpec):
            self.__initialize_dataflow_pipeline(spark, dataflow_spec, view_name, view_name_quarantine)
        else:
            raise Exception("Dataflow not supported!")

    def __initialize_dataflow_pipeline(self, spark, dataflow_spec, view_name, view_name_quarantine):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                # self.schema_json = None
                self.schema_json = DataflowSpecUtils.get_schema_json(self.spark, self.dataflowSpec)
        else:
            self.schema_json = None
        if isinstance(dataflow_spec, SilverDataflowSpec) and not self.dataflowSpec.materializedView:
            self.silver_schema = self.get_silver_schema()
        else:
            self.silver_schema = None

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def read(self):
        """Read DLT."""
        logger.info("In read function")
    
        if isinstance(self.dataflowSpec, BronzeDataflowSpec):
            dlt.view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for{self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            silverDataflowSpec: SilverDataflowSpec = self.dataflowSpec
            

            ## If there are DQEs in Silver layer,
            ## Create a second view for DQE operation
            
            if silverDataflowSpec.dataQualityExpectations:
                data_quality_expectations_json = json.loads(silverDataflowSpec.dataQualityExpectations)
                expect_dict = None
                expect_or_fail_dict = None
                expect_or_drop_dict = None
                dlt_table_with_expectation = None
                select_exp = silverDataflowSpec.selectExp
                where_clause = silverDataflowSpec.whereClause

                if "expect_all" in data_quality_expectations_json:
                    expect_dict = data_quality_expectations_json["expect_all"]
                if "expect" in data_quality_expectations_json:
                    expect_dict = data_quality_expectations_json["expect"]
                if "expect_or_fail" in data_quality_expectations_json:
                    expect_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
                if "expect_all_or_fail" in data_quality_expectations_json:
                    expect_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
                if "expect_or_drop" in data_quality_expectations_json:
                    expect_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
                if "expect_all_or_drop" in data_quality_expectations_json:
                    expect_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
                if "expect_or_quarantine" in data_quality_expectations_json:
                    expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        
                
                if expect_dict:
                    dlt_view_with_expectation = dlt.expect_all(expect_dict)(
                        dlt.view(
                        self.read_silver_with_dqe,
                        name=self.view_name+"_DQE",
                        comment=f"input dataset view for{self.view_name}",
                        )
                    )
                if expect_or_fail_dict:
                    if expect_dict is None:
                        dlt_view_with_expectation = dlt.expect_all_or_fail(expect_or_fail_dict)(
                        dlt.view(
                        self.read_silver_with_dqe,
                        name=self.view_name+"_DQE",
                        comment=f"input dataset view for{self.view_name}",
                        )
                    )
                    else:
                        dlt_view_with_expectation = dlt.expect_all_or_fail(expect_or_fail_dict)(dlt_table_with_expectation)
                if expect_or_drop_dict:
                    if expect_dict is None and expect_or_fail_dict is None:
                        dlt_view_with_expectation = dlt.expect_all_or_drop(expect_or_drop_dict)(
                        dlt.view(
                        self.read_silver_with_dqe,
                        name=self.view_name+"_DQE",
                        comment=f"input dataset view for{self.view_name}",
                        )
                    )
                    else:
                        dlt_view_with_expectation = dlt.expect_all_or_drop(expect_or_drop_dict)(dlt_table_with_expectation)
            ## Create a view to read data
            ## from Bronze layer OR from DQE view
            ## If it is a materialized view
            ## There is no need to read
            ## and will be done using SQL in the next step
            if not silverDataflowSpec.materializedView:    
                
                dlt.view(
                    self.read_silver,
                    name=self.view_name,
                    comment=f"input dataset view for{self.view_name}",
                )
        else:
            raise Exception("Dataflow read not supported for{}".format(type(self.dataflowSpec)))

    def write(self):
        """Write DLT."""
        if isinstance(self.dataflowSpec, BronzeDataflowSpec):
            self.write_bronze()
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            self.write_silver()
        else:
            raise Exception(f"Dataflow write not supported for type= {type(self.dataflowSpec)}")

    def write_bronze(self):
        """Write Bronze tables."""
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        if bronze_dataflow_spec.dataQualityExpectations:
            self.write_bronze_with_dqe()
        elif bronze_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronze_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=f"{bronze_dataflow_spec.targetDetails['table']}",
                partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(bronze_dataflow_spec.liquidClusteringColumns),
                table_properties=bronze_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"bronze dlt table{bronze_dataflow_spec.targetDetails['table']}",
            )

    def write_silver(self):
        """Write silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        if silver_dataflow_spec.materializedView:
            self.create_materialized_view()
        elif silver_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        # elif silver_dataflow_spec.dataQualityExpectations:
        #     self.write_silver_with_dqe()
        else:
            target_path = None if self.uc_enabled else silver_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=f"{silver_dataflow_spec.targetDetails['table']}",
                partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(silver_dataflow_spec.liquidClusteringColumns),
                table_properties=silver_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"silver dlt table{silver_dataflow_spec.targetDetails['table']}",
            )

    def read_bronze(self) -> DataFrame:
        """Read Bronze Table."""
        logger.info("In read_bronze func")
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        if bronze_dataflow_spec.sourceFormat == "cloudFiles":
            return PipelineReaders.read_dlt_cloud_files(self.spark, bronze_dataflow_spec, self.schema_json)
        elif bronze_dataflow_spec.sourceFormat == "delta":
            return PipelineReaders.read_dlt_delta(self.spark, bronze_dataflow_spec)
        elif bronze_dataflow_spec.sourceFormat == "eventhub" or bronze_dataflow_spec.sourceFormat == "kafka":
            return PipelineReaders.read_kafka(self.spark, bronze_dataflow_spec, self.schema_json)
        else:
            raise Exception(f"{bronze_dataflow_spec.sourceFormat} source format not supported")

    def get_silver_schema(self):
        """Get Silver table Schema."""
        
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        mv_query = silver_dataflow_spec.materializedView
        if silver_dataflow_spec.materializedView:
            raw_delta_table_stream = self.spark.sql(mv_query)
        else:
            raw_delta_table_stream = self.spark.readStream.table(
                f"{source_database}.{source_table}"
            ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
             path=silver_dataflow_spec.sourceDetails["path"],
                format="delta"
            ).selectExpr(*select_exp)
            raw_delta_table_stream = self.__apply_where_clause(where_clause, raw_delta_table_stream)
        return raw_delta_table_stream.schema

    def __apply_where_clause(self, where_clause, raw_delta_table_stream):
        """This method apply where clause provided in silver transformations

        Args:
            where_clause (_type_): _description_
            raw_delta_table_stream (_type_): _description_

        Returns:
            _type_: _description_
        """
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)
        return raw_delta_table_stream
    ## CREATES THE Initial view from Bronze
    ## only if there are DQEs
    def read_silver_with_dqe(self) -> DataFrame:
        """Read Silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        )
        return raw_delta_table_stream
    ## CREATES Second view to apply Select Expression
    ## And Where_clause    
    def read_silver(self) -> DataFrame:
        """Read Silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause

        ## If there are DQEs read from DQE view
        if silver_dataflow_spec.dataQualityExpectations:
            raw_delta_table_stream = dlt.read_stream(self.view_name+"_DQE"
            ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
                path=silver_dataflow_spec.sourceDetails["path"],
                format="delta"
            ).selectExpr(*select_exp)
        ## If there no DQEs read from Bronze table
        else:
            raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
                path=silver_dataflow_spec.sourceDetails["path"],
                format="delta"
            ).selectExpr(*select_exp)
        
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)
        return raw_delta_table_stream

    def write_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(self.view_name)

    def write_bronze_with_dqe(self):
        """Write Bronze table with data quality expectations."""
        bronzeDataflowSpec: BronzeDataflowSpec = self.dataflowSpec
        data_quality_expectations_json = json.loads(bronzeDataflowSpec.dataQualityExpectations)
        expect_dict = None
        expect_or_fail_dict = None
        expect_or_drop_dict = None
        dlt_table_with_expectation = None
        expect_or_quarantine_dict = None

        if "expect_all" in data_quality_expectations_json:
            expect_dict = data_quality_expectations_json["expect_all"]
        if "expect" in data_quality_expectations_json:
            expect_dict = data_quality_expectations_json["expect"]
        if "expect_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        if "expect_all_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
        if "expect_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
        if "expect_all_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        if bronzeDataflowSpec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronzeDataflowSpec.targetDetails["path"]
            if expect_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{bronzeDataflowSpec.targetDetails['table']}",
                        table_properties=bronzeDataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(bronzeDataflowSpec.liquidClusteringColumns),
                        path=target_path,
                        comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                    )
                )
            if expect_or_fail_dict:
                if expect_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{bronzeDataflowSpec.targetDetails['table']}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(bronzeDataflowSpec.liquidClusteringColumns),
                            path=target_path,
                            comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_or_fail_dict)(dlt_table_with_expectation)
            if expect_or_drop_dict:
                if expect_dict is None and expect_or_fail_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_or_drop_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{bronzeDataflowSpec.targetDetails['table']}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(bronzeDataflowSpec.liquidClusteringColumns),
                            path=target_path,
                            comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_or_drop_dict)(dlt_table_with_expectation)
            if expect_or_quarantine_dict:
                q_partition_cols = None
                if (
                    "partition_columns" in bronzeDataflowSpec.quarantineTargetDetails
                    and bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]
                ):
                    q_partition_cols = [bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]]
                target_path = None if self.uc_enabled else bronzeDataflowSpec.quarantineTargetDetails["path"]
                dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{bronzeDataflowSpec.quarantineTargetDetails['table']}",
                        table_properties=bronzeDataflowSpec.quarantineTableProperties,
                        partition_cols=q_partition_cols,
                        cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(bronzeDataflowSpec.liquidClusteringColumns),
                        path=target_path,
                        comment=f"""bronze dlt quarantine_path table
                        {bronzeDataflowSpec.quarantineTargetDetails['table']}""",
                    )
                )
        
    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        struct_schema = (
            StructType.fromJson(self.schema_json)
            if isinstance(self.dataflowSpec, BronzeDataflowSpec)
            else self.silver_schema
        )

        sequenced_by_data_type = None
        sequenced_by_struct = StructType([])
        modified_schema = StructType([])
        if struct_schema:
            for field in struct_schema.fields:
                if cdc_apply_changes.except_column_list:
                    if field.name not in cdc_apply_changes.except_column_list:
                        modified_schema.add(field)
                else:
                    modified_schema.add(field)                    
                if len(cdc_apply_changes.sequence_by) == 1 and field.name == cdc_apply_changes.sequence_by[0]:
                    sequenced_by_data_type = field.dataType
                else:
                    if field.name in cdc_apply_changes.sequence_by:
                        sequenced_by_struct.add(StructField(field.name, field.dataType))
            struct_schema = modified_schema

        if struct_schema and cdc_apply_changes.scd_type == "2":
            if sequenced_by_data_type:
                struct_schema.add(StructField("__START_AT", sequenced_by_data_type))
                struct_schema.add(StructField("__END_AT", sequenced_by_data_type))
            else:
                struct_schema.add(StructField("__START_AT", sequenced_by_struct))
                struct_schema.add(StructField("__END_AT", sequenced_by_struct))           


        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        
        dlt.create_streaming_table(
            name=f"{self.dataflowSpec.targetDetails['table']}",
            table_properties=self.dataflowSpec.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_liquid_clustering_cols(self.dataflowSpec.liquidClusteringColumns),
            path=target_path,
            schema=struct_schema
        )

        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)

        dlt.apply_changes(
            target=f"{self.dataflowSpec.targetDetails['table']}",
            source=self.view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=struct(*cdc_apply_changes.sequence_by) if len(cdc_apply_changes.sequence_by) != 1 else col(cdc_apply_changes.sequence_by[0]),
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list

        )

    def create_materialized_view(self):
        """Create Materialized View."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        mv_query = silver_dataflow_spec.materializedView
        @dlt.table(
            spark_conf={"pipelines.trigger.interval" : "60 seconds"},
            name=f"{silver_dataflow_spec.targetDetails['table']}",
            table_properties=silver_dataflow_spec.tableProperties,
            comment=f"silver dlt Materialized View {silver_dataflow_spec.targetDetails['table']} - 60 seconds refresh",
        )
        def create_mv():
            return self.spark.sql(mv_query)

    

    def run_dlt(self):
        """Run DLT."""
        logger.info("in run_dlt function")
        self.read()
        self.write()

    @staticmethod
    def invoke_dlt_pipeline(spark, layer):
        """Invoke dlt pipeline will launch dlt with given dataflowspec.
        

        Args:
            spark (_type_): _description_
            layer (_type_): _description_
        """
        dataflowspec_list = None
        if "bronze" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
        elif "silver" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
        logger.info("Length of Dataflow Spec %d", len(dataflowspec_list))
        for dataflowSpec in dataflowspec_list:
            logger.info("Printing Dataflow Spec")
            logger.info(dataflowSpec)
            quarantine_input_view_name = None
            if isinstance(dataflowSpec, BronzeDataflowSpec) and dataflowSpec.quarantineTargetDetails is not None \
                    and dataflowSpec.quarantineTargetDetails != {}:
                quarantine_input_view_name = (
                    f"{dataflowSpec.quarantineTargetDetails['table']}"
                    f"_{layer}_quarantine_inputView"
                )
            else:
                logger.info("quarantine_input_view_name set to None")

            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                f"{dataflowSpec.targetDetails['table']}_{layer}_inputView",
                quarantine_input_view_name,
            )

            dlt_data_flow.run_dlt()
    