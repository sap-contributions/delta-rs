#![cfg(feature = "datafusion")]
use std::collections::HashSet;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use datafusion::assert_batches_sorted_eq;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::ScalarValue::*;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{common::collect, metrics::Label};
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};
use deltalake_core::delta_datafusion::{DeltaScan, DeltaTableFactory};
use deltalake_core::kernel::{DataType, MapType, PrimitiveType, StructField, StructType};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::operations::write::SchemaMode;
use deltalake_core::protocol::SaveMode;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::{
    open_table,
    operations::{write::WriteBuilder, DeltaOps},
    DeltaTable, DeltaTableError,
};
use deltalake_test::utils::*;
use serial_test::serial;
use url::Url;

pub fn context_with_delta_table_factory() -> SessionContext {
    let mut state = SessionStateBuilder::new().build();
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    SessionContext::new_with_state(state)
}

mod local {
    use super::*;
    use datafusion::common::assert_contains;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{
        lit, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableScan,
    };
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use datafusion::{common::stats::Precision, datasource::provider_as_source};
    use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
    use deltalake_core::{
        delta_datafusion::DeltaLogicalCodec, logstore::default_logstore, writer::JsonWriter,
    };
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use TableProviderFilterPushDown::{Exact, Inexact};
    #[tokio::test]
    #[serial]
    async fn test_datafusion_local() -> TestResult {
        let storage = Box::<LocalStorageIntegration>::default();
        let context = IntegrationContext::new(storage)?;
        test_datafusion(&context).await
    }

    fn get_scanned_files(node: &dyn ExecutionPlan) -> HashSet<Label> {
        node.metrics()
            .unwrap()
            .iter()
            .flat_map(|m| m.labels().to_vec())
            .collect()
    }

    #[derive(Debug, Default)]
    pub struct ExecutionMetricsCollector {
        scanned_files: HashSet<Label>,
        pub skip_count: usize,
        pub keep_count: usize,
    }

    impl ExecutionMetricsCollector {
        fn num_scanned_files(&self) -> usize {
            self.scanned_files.len()
        }
    }

    impl ExecutionPlanVisitor for ExecutionMetricsCollector {
        type Error = DataFusionError;

        fn pre_visit(
            &mut self,
            plan: &dyn ExecutionPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
                let files = get_scanned_files(exec);
                self.scanned_files.extend(files);
            } else if let Some(exec) = plan.as_any().downcast_ref::<DeltaScan>() {
                self.keep_count = exec
                    .metrics()
                    .and_then(|m| m.sum_by_name("files_scanned").map(|v| v.as_usize()))
                    .unwrap_or_default();
                self.skip_count = exec
                    .metrics()
                    .and_then(|m| m.sum_by_name("files_pruned").map(|v| v.as_usize()))
                    .unwrap_or_default();
            }
            Ok(true)
        }
    }

    async fn prepare_table(
        batches: Vec<RecordBatch>,
        save_mode: SaveMode,
        partitions: Vec<String>,
    ) -> (tempfile::TempDir, DeltaTable) {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();
        let table_schema: StructType = batches[0].schema().try_into_kernel().unwrap();

        let mut table = DeltaOps::try_from_uri(table_uri)
            .await
            .unwrap()
            .create()
            .with_save_mode(SaveMode::Ignore)
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(partitions)
            .await
            .unwrap();

        for batch in batches {
            table = DeltaOps(table)
                .write(vec![batch])
                .with_save_mode(save_mode)
                .await
                .unwrap();
        }

        (table_dir, table)
    }

    #[tokio::test]
    async fn test_datafusion_sql_registration() -> Result<()> {
        let ctx = context_with_delta_table_factory();

        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("../test/tests/data/delta-0.8.0-partitioned");
        let sql = format!(
            "CREATE EXTERNAL TABLE demo STORED AS DELTATABLE LOCATION '{}'",
            d.to_str().unwrap()
        );
        let _ = ctx
            .sql(sql.as_str())
            .await
            .expect("Failed to register table!");

        let batches = ctx
        .sql("SELECT CAST( day as int ) as my_day FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
        .await?
        .collect()
        .await?;

        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_simple_query_partitioned() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/delta-0.8.0-partitioned")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT CAST( day as int ) as my_day FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
            .await?
            .collect()
            .await?;

        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
        );

        Ok(())
    }

    struct PruningTestCase {
        sql: String,
        push_down: Vec<TableProviderFilterPushDown>,
    }

    impl PruningTestCase {
        fn new(sql: &str) -> Self {
            Self {
                sql: sql.to_string(),
                push_down: vec![Exact],
            }
        }

        fn with_push_down(sql: &str, push_down: Vec<TableProviderFilterPushDown>) -> Self {
            Self {
                sql: sql.to_string(),
                push_down,
            }
        }
    }

    #[tokio::test]
    async fn test_datafusion_optimize_stats_partitioned_pushdown() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(2);
        let ctx = SessionContext::new_with_config(config);
        let table = open_table("../test/tests/data/http_requests").await?;
        ctx.register_table("http_requests", Arc::new(table.clone()))?;

        let sql = "SELECT COUNT(*) as num_events FROM http_requests WHERE date > '2023-04-13'";
        let df = ctx.sql(sql).await?;
        let plan = df.clone().create_physical_plan().await?;

        // convert to explain plan form
        let display = displayable(plan.as_ref()).indent(true).to_string();

        assert_contains!(
            &display,
            "ProjectionExec: expr=[1437 as num_events]\n  PlaceholderRowExec"
        );

        let batches = df.collect().await?;
        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int64Array::from(vec![1437])).as_ref(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_query_partitioned_pushdown() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/delta-0.8.0-partitioned").await?;
        ctx.register_table("demo", Arc::new(table.clone()))?;

        let pruning_predicates = [
            PruningTestCase::new("year > '2020'"),
            PruningTestCase::new("year != '2020'"),
            PruningTestCase::new("year = '2021'"),
            PruningTestCase::with_push_down(
                "year = '2021' AND day IS NOT NULL",
                vec![Exact, Exact],
            ),
            PruningTestCase::new("year IN ('2021', '2022')"),
            // NOT IN (a, b) is rewritten as (col != a AND col != b)
            PruningTestCase::with_push_down("year NOT IN ('2020', '2022')", vec![Exact, Exact]),
            // BETWEEN a AND b is rewritten as (col >= a AND col < b)
            PruningTestCase::with_push_down("year BETWEEN '2021' AND '2022'", vec![Exact, Exact]),
            PruningTestCase::new("year NOT BETWEEN '2019' AND '2020'"),
            PruningTestCase::with_push_down(
                "year = '2021' AND day IN ('4', '5', '20')",
                vec![Exact, Exact],
            ),
            PruningTestCase::with_push_down(
                "year = '2021' AND cast(day as int) <= 20",
                vec![Exact, Inexact],
            ),
        ];

        fn find_scan_filters(plan: &LogicalPlan) -> Vec<&Expr> {
            let mut result = vec![];

            plan.apply(|node| {
                if let LogicalPlan::TableScan(TableScan { ref filters, .. }) = node {
                    result = filters.iter().collect();
                    Ok(TreeNodeRecursion::Stop) // Stop traversal once found
                } else {
                    Ok(TreeNodeRecursion::Continue) // Continue traversal
                }
            })
            .expect("Traversal should not fail");

            result
        }

        for pp in pruning_predicates {
            let pred = pp.sql;
            let sql = format!("SELECT CAST( day as int ) as my_day FROM demo WHERE {pred} ORDER BY CAST( day as int ) ASC");
            println!("\nExecuting query: {sql}");

            let df = ctx.sql(sql.as_str()).await?;

            // validate that we are correctly qualifying filters as Exact or Inexact
            let plan = df.clone().into_optimized_plan()?;
            let filters = find_scan_filters(&plan);
            let push_down = table.supports_filters_pushdown(&filters)?;

            assert_eq!(push_down, pp.push_down);

            let batches = df.collect().await?;

            let batch = &batches[0];

            assert_eq!(
                batch.column(0).as_ref(),
                Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_files_scanned_pushdown_limit() -> Result<()> {
        use datafusion::prelude::*;
        let ctx = SessionContext::new();
        let state = ctx.state();
        let table = open_table("../test/tests/data/delta-0.8.0").await?;

        // Simple Equality test, we only exercise the limit in this test
        let e = col("value").eq(lit(2));
        let metrics = get_scan_metrics(&table, &state, &[e.clone()]).await?;
        assert_eq!(metrics.num_scanned_files(), 2);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 0);

        let metrics = get_scan_metrics_with_limit(&table, &state, &[e.clone()], Some(1)).await?;
        assert_eq!(metrics.num_scanned_files(), 1);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 1);

        let metrics = get_scan_metrics_with_limit(&table, &state, &[e.clone()], Some(3)).await?;
        assert_eq!(metrics.num_scanned_files(), 2);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_write_from_serialized_delta_scan() -> Result<()> {
        // Build an execution plan for scanning a DeltaTable and serialize it to bytes.
        // We want to emulate that this occurs on another node, so that all we have access to is the
        // plan byte serialization.
        let source_scan_bytes = {
            let source_table = open_table("../test/tests/data/delta-0.8.0-date").await?;

            let target_provider = provider_as_source(Arc::new(source_table));
            let source =
                LogicalPlanBuilder::scan("source", target_provider.clone(), None)?.build()?;
            // We don't want to verify the predicate against existing data
            logical_plan_to_bytes_with_extension_codec(&source, &DeltaLogicalCodec {})?
        };

        // Build a new context from scratch and deserialize the plan
        let ctx = SessionContext::new();
        let state = ctx.state();
        let source_scan = Arc::new(logical_plan_from_bytes_with_extension_codec(
            &source_scan_bytes,
            &ctx,
            &DeltaLogicalCodec {},
        )?);
        let schema: StructType = source_scan.schema().as_arrow().try_into_kernel().unwrap();
        let fields = schema.fields().cloned();

        dbg!(schema.fields().collect_vec().clone());
        // Create target Delta Table
        let target_table = CreateBuilder::new()
            .with_location("memory:///target")
            .with_columns(fields)
            .with_table_name("target")
            .await?;

        // Execute write to the target table with the proper state
        let target_table = WriteBuilder::new(
            target_table.log_store(),
            target_table.snapshot().ok().cloned(),
        )
        .with_input_execution_plan(source_scan)
        .with_input_session_state(state)
        .await?;
        ctx.register_table("target", Arc::new(target_table))?;

        // Check results
        let batches = ctx.sql("SELECT * FROM target").await?.collect().await?;
        let expected = [
            "+------------+-----------+",
            "| date       | dayOfYear |",
            "+------------+-----------+",
            "| 2021-01-01 | 1         |",
            "| 2021-01-02 | 2         |",
            "| 2021-01-03 | 3         |",
            "| 2021-01-04 | 4         |",
            "| 2021-01-05 | 5         |",
            "+------------+-----------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_date_column() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/delta-0.8.0-date")
            .await
            .unwrap();
        ctx.register_table("dates", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT date from dates WHERE \"dayOfYear\" = 2")
            .await?
            .collect()
            .await?;

        assert_eq!(
            batches[0].column(0).as_ref(),
            Arc::new(Date32Array::from(vec![18629])).as_ref(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_stats() -> Result<()> {
        // Validate a table that contains statistics for all files
        let table = open_table("../test/tests/data/delta-0.8.0").await.unwrap();
        let statistics = table.snapshot()?.datafusion_table_statistics().unwrap();

        assert_eq!(statistics.num_rows, Precision::Exact(4));

        assert_eq!(
            statistics.total_byte_size,
            Precision::Inexact((440 + 440) as usize)
        );
        let column_stats = statistics.column_statistics.first().unwrap();
        assert_eq!(column_stats.null_count, Precision::Exact(0));
        assert_eq!(
            column_stats.max_value,
            Precision::Exact(ScalarValue::from(4_i32))
        );
        assert_eq!(
            column_stats.min_value,
            Precision::Exact(ScalarValue::from(0_i32))
        );

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table))?;
        let actual = ctx
            .sql("SELECT max(value), min(value) FROM test_table")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+-----------------------+-----------------------+",
            "| max(test_table.value) | min(test_table.value) |",
            "+-----------------------+-----------------------+",
            "| 4                     | 0                     |",
            "+-----------------------+-----------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate a table that does not contain column statistics
        let table = open_table("../test/tests/data/delta-0.2.0").await.unwrap();
        let statistics = table.snapshot()?.datafusion_table_statistics().unwrap();

        assert_eq!(statistics.num_rows, Precision::Absent);

        assert_eq!(
            statistics.total_byte_size,
            Precision::Inexact((400 + 404 + 396) as usize)
        );
        let column_stats = statistics.column_statistics.first().unwrap();
        assert_eq!(column_stats.null_count, Precision::Absent);
        assert_eq!(column_stats.max_value, Precision::Absent);
        assert_eq!(column_stats.min_value, Precision::Absent);

        ctx.register_table("test_table2", Arc::new(table))?;
        let actual = ctx
            .sql("SELECT max(value), min(value) FROM test_table2")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+------------------------+------------------------+",
            "| max(test_table2.value) | min(test_table2.value) |",
            "+------------------------+------------------------+",
            "| 3                      | 1                      |",
            "+------------------------+------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate a table that contains nested structures.

        // This table is interesting since it goes through schema evolution.
        // In particular 'new_column' contains statistics for when it
        // is introduced (10) but the commit following (11) does not contain
        // statistics for this column.
        let table = open_table("../test/tests/data/delta-1.2.1-only-struct-stats")
            .await
            .unwrap();
        let schema = table.get_schema().unwrap();
        let statistics = table.snapshot()?.datafusion_table_statistics().unwrap();
        assert_eq!(statistics.num_rows, Precision::Exact(12));

        // `new_column` statistics
        let stats = statistics
            .column_statistics
            .get(schema.index_of("new_column").unwrap())
            .unwrap();
        assert_eq!(stats.null_count, Precision::Absent);
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);

        // `date` statistics
        let stats = statistics
            .column_statistics
            .get(schema.index_of("date").unwrap())
            .unwrap();
        assert_eq!(stats.null_count, Precision::Exact(0));
        // 2022-10-24
        assert_eq!(
            stats.min_value,
            Precision::Exact(ScalarValue::Date32(Some(19289)))
        );
        assert_eq!(
            stats.max_value,
            Precision::Exact(ScalarValue::Date32(Some(19289)))
        );

        // `timestamp` statistics
        let stats = statistics
            .column_statistics
            .get(schema.index_of("timestamp").unwrap())
            .unwrap();
        assert_eq!(stats.null_count, Precision::Exact(0));
        // 2022-10-24T22:59:32.846Z
        assert_eq!(
            stats.min_value,
            Precision::Exact(ScalarValue::TimestampMicrosecond(
                Some(1666652372846000),
                None
            ))
        );
        // 2022-10-24T22:59:46.083Z
        assert_eq!(
            stats.max_value,
            Precision::Exact(ScalarValue::TimestampMicrosecond(
                Some(1666652386083000),
                None
            ))
        );

        // `struct_element` statistics
        let stats = statistics
            .column_statistics
            .get(schema.index_of("nested_struct").unwrap())
            .unwrap();
        assert_eq!(stats.null_count, Precision::Absent);
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);

        Ok(())
    }

    async fn get_scan_metrics_with_limit(
        table: &DeltaTable,
        state: &SessionState,
        e: &[Expr],
        limit: Option<usize>,
    ) -> Result<ExecutionMetricsCollector> {
        let mut metrics = ExecutionMetricsCollector::default();
        let scan = table.scan(state, None, e, limit).await?;
        if scan.properties().output_partitioning().partition_count() > 0 {
            let plan = CoalescePartitionsExec::new(scan);
            let task_ctx = Arc::new(TaskContext::from(state));
            let _result = collect(plan.execute(0, task_ctx)?).await?;
            visit_execution_plan(&plan, &mut metrics).unwrap();
        } else {
            // if scan produces no output from ParquetExec, we still want to visit DeltaScan
            // to check its metrics
            visit_execution_plan(scan.as_ref(), &mut metrics).unwrap();
        }

        Ok(metrics)
    }

    async fn get_scan_metrics(
        table: &DeltaTable,
        state: &SessionState,
        e: &[Expr],
    ) -> Result<ExecutionMetricsCollector> {
        get_scan_metrics_with_limit(table, state, e, None).await
    }

    fn create_all_types_batch(
        not_null_rows: usize,
        null_rows: usize,
        offset: usize,
    ) -> RecordBatch {
        let mut decimal_builder = Decimal128Builder::with_capacity(not_null_rows + null_rows);
        for x in 0..not_null_rows {
            decimal_builder.append_value(((x + offset) * 100) as i128);
        }
        decimal_builder.append_nulls(null_rows);
        let decimal = decimal_builder
            .finish()
            .with_precision_and_scale(10, 2)
            .unwrap();

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset).to_string()))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Int64Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as i64))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Int32Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as i32))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Int16Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as i16))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Int8Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as i8))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Float64Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as f64))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Float32Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as f32))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(BooleanArray::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) % 2 == 0))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(BinaryArray::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset).to_string().as_bytes().to_owned()))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(decimal),
            //Convert to seconds
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..not_null_rows)
                    .map(|x| Some(((x + offset) * 1_000_000) as i64))
                    .chain((0..null_rows).map(|_| None)),
            )),
            Arc::new(Date32Array::from_iter(
                (0..not_null_rows)
                    .map(|x| Some((x + offset) as i32))
                    .chain((0..null_rows).map(|_| None)),
            )),
        ];

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("utf8", ArrowDataType::Utf8, true),
            ArrowField::new("int64", ArrowDataType::Int64, true),
            ArrowField::new("int32", ArrowDataType::Int32, true),
            ArrowField::new("int16", ArrowDataType::Int16, true),
            ArrowField::new("int8", ArrowDataType::Int8, true),
            ArrowField::new("float64", ArrowDataType::Float64, true),
            ArrowField::new("float32", ArrowDataType::Float32, true),
            ArrowField::new("boolean", ArrowDataType::Boolean, true),
            ArrowField::new("binary", ArrowDataType::Binary, true),
            ArrowField::new("decimal", ArrowDataType::Decimal128(10, 2), true),
            ArrowField::new(
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            ArrowField::new("date", ArrowDataType::Date32, true),
        ]));

        RecordBatch::try_new(schema, data).unwrap()
    }

    struct TestCase {
        column: &'static str,
        file1_value: Expr,
        file2_value: Expr,
        file3_value: Expr,
        non_existent_value: Expr,
    }

    impl TestCase {
        fn new<F>(column: &'static str, expression_builder: F) -> Self
        where
            F: Fn(i64) -> Expr,
        {
            TestCase {
                column,
                file1_value: expression_builder(1),
                file2_value: expression_builder(5),
                file3_value: expression_builder(8),
                non_existent_value: expression_builder(3),
            }
        }

        fn new_wrapped<F>(column: &'static str, expression_builder: F) -> Self
        where
            F: Fn(i64) -> Expr,
        {
            TestCase {
                column,
                file1_value: wrap_expression(expression_builder(1)),
                file2_value: wrap_expression(expression_builder(5)),
                file3_value: wrap_expression(expression_builder(8)),
                non_existent_value: wrap_expression(expression_builder(3)),
            }
        }
    }

    fn wrap_expression(e: Expr) -> Expr {
        let value = match e {
            Expr::Literal(lit, _) => lit,
            _ => unreachable!(),
        };
        lit(ScalarValue::Dictionary(
            Box::new(ArrowDataType::UInt16),
            Box::new(value),
        ))
    }

    #[tokio::test]
    async fn test_files_scanned() -> Result<()> {
        // Validate that datafusion prunes files based on file statistics
        // Do not scan files when we know it does not contain the requested records
        use datafusion::prelude::*;
        let ctx = SessionContext::new();
        let state = ctx.state();

        async fn append_to_table(table: DeltaTable, batch: RecordBatch) -> DeltaTable {
            DeltaOps(table)
                .write(vec![batch])
                .with_save_mode(SaveMode::Append)
                .await
                .unwrap()
        }

        let batch = create_all_types_batch(3, 0, 0);
        let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, vec![]).await;

        let batch = create_all_types_batch(3, 0, 4);
        let table = append_to_table(table, batch).await;

        let batch = create_all_types_batch(3, 0, 7);
        let table = append_to_table(table, batch).await;

        let metrics = get_scan_metrics(&table, &state, &[]).await?;
        assert_eq!(metrics.num_scanned_files(), 3);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 0);

        // (Column name, value from file 1, value from file 2, value from file 3, non existent value)
        let tests = [
            TestCase::new("utf8", |value| lit(value.to_string())),
            TestCase::new("int64", lit),
            TestCase::new("int32", |value| lit(value as i32)),
            TestCase::new("int16", |value| lit(value as i16)),
            TestCase::new("int8", |value| lit(value as i8)),
            TestCase::new("float64", |value| lit(value as f64)),
            TestCase::new("float32", |value| lit(value as f32)),
            TestCase::new("timestamp", |value| {
                lit(TimestampMicrosecond(Some(value * 1_000_000), None))
            }),
            // TODO: I think decimal statistics are being written to the log incorrectly. The underlying i128 is written
            // not the proper string representation as specified by the precision and scale
            TestCase::new("decimal", |value| {
                lit(Decimal128(Some((value * 100).into()), 10, 2))
            }),
            // TODO: The writer does not write complete statistics for date columns
            TestCase::new("date", |value| lit(Date32(Some(value as i32)))),
            // TODO: The writer does not write complete statistics for binary columns
            TestCase::new("binary", |value| lit(value.to_string().as_bytes())),
        ];

        for test in &tests {
            let TestCase {
                column,
                file1_value,
                file2_value,
                file3_value,
                non_existent_value,
            } = test.to_owned();
            let column = column.to_owned();
            // TODO: The following types don't have proper stats written.
            // See issue #1208 for decimal type
            // See issue #1209 for dates
            // Min and Max is not calculated for binary columns. This matches the Spark writer
            if column == "decimal" || column == "date" || column == "binary" {
                continue;
            }
            println!("[Unwrapped] Test Column: {column} value: {file1_value}");

            // Equality
            let e = col(column).eq(file1_value.clone());
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 1);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 2);

            // Value does not exist
            let e = col(column).eq(non_existent_value.clone());
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 0);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 3);

            // Conjunction
            let e = col(column)
                .gt(file1_value.clone())
                .and(col(column).lt(file2_value.clone()));
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 2);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 1);

            // Disjunction
            let e = col(column)
                .lt(file1_value.clone())
                .or(col(column).gt(file3_value.clone()));
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 2);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 1);
        }

        // Validate Boolean type
        let batch = create_all_types_batch(1, 0, 0);
        let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, vec![]).await;
        let batch = create_all_types_batch(1, 0, 1);
        let table = append_to_table(table, batch).await;

        let e = col("boolean").eq(lit(true));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 1);

        let e = col("boolean").eq(lit(false));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 1);

        let tests = [
            TestCase::new_wrapped("utf8", |value| lit(value.to_string())),
            TestCase::new_wrapped("int64", lit),
            TestCase::new_wrapped("int32", |value| lit(value as i32)),
            TestCase::new_wrapped("int16", |value| lit(value as i16)),
            TestCase::new_wrapped("int8", |value| lit(value as i8)),
            TestCase::new_wrapped("float64", |value| lit(value as f64)),
            TestCase::new_wrapped("float32", |value| lit(value as f32)),
            TestCase::new_wrapped("timestamp", |value| {
                lit(TimestampMicrosecond(Some(value * 1_000_000), None))
            }),
            // TODO: I think decimal statistics are being written to the log incorrectly. The underlying i128 is written
            // not the proper string representation as specified by the precision and scale
            TestCase::new_wrapped("decimal", |value| {
                lit(Decimal128(Some((value * 100).into()), 10, 2))
            }),
            // TODO: The writer does not write complete statistics for date columns
            TestCase::new_wrapped("date", |value| lit(Date32(Some(value as i32)))),
            // TODO: The writer does not write complete statistics for binary columns
            TestCase::new_wrapped("binary", |value| lit(value.to_string().as_bytes())),
        ];

        // Ensure that tables with stats and partition columns can be pruned
        for test in tests {
            let TestCase {
                column,
                file1_value,
                file2_value,
                file3_value,
                non_existent_value,
            } = test;
            // TODO: Float and decimal partitions are not supported by the writer
            // binary fails since arrow does not implement a natural order
            // The current Datafusion pruning implementation does not work for binary columns since they do not have a natural order. See #1214
            // Timestamp and date are disabled since the hive path contains illegal Windows values. see #1215
            if column == "int64"
                || column == "int32"
                || column == "int16"
                || column == "int8"
                || column == "float32"
                || column == "float64"
                || column == "decimal"
                || column == "binary"
                || column == "timestamp"
                || column == "date"
            {
                continue;
            }

            println!("[Wrapped] Test Column: {column} value: {file1_value}");

            let partitions = vec![column.to_owned()];
            let batch = create_all_types_batch(3, 0, 0);
            let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, partitions).await;

            let batch = create_all_types_batch(3, 0, 4);
            let table = append_to_table(table, batch).await;

            let batch = create_all_types_batch(3, 0, 7);
            let table = append_to_table(table, batch).await;

            // Equality
            let e = col(column).eq(file1_value.clone());
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 1);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 8);

            // Value does not exist
            let e = col(column).eq(non_existent_value);
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 0);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 9);

            // Conjunction
            let e = col(column)
                .gt(file1_value.clone())
                .and(col(column).lt(file2_value));
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 2);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 7);

            // Disjunction
            let e = col(column).lt(file1_value).or(col(column).gt(file3_value));
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 2);
            assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
            assert_eq!(metrics.skip_count, 7);

            // TODO how to get an expression with the right datatypes eludes me ..
            // Validate null pruning
            // let batch = create_all_types_batch(5, 2, 0);
            // let partitions = vec![column.to_owned()];
            // let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, partitions).await;

            // let e = col(column).is_null();
            // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            // assert_eq!(metrics.num_scanned_files(), 1);

            /*  logically we should be able to prune the null partition but Datafusion's current implementation prevents this */
            /*
            let e = col(column).is_not_null();
            let metrics = get_scan_metrics(&table, &state, &[e]).await?;
            assert_eq!(metrics.num_scanned_files(), 5);
            */
        }

        // Validate Boolean partition
        let batch = create_all_types_batch(1, 0, 0);
        let (_tmp, table) =
            prepare_table(vec![batch], SaveMode::Overwrite, vec!["boolean".to_owned()]).await;
        let batch = create_all_types_batch(1, 0, 1);
        let table = append_to_table(table, batch).await;

        let e = col("boolean").eq(lit(true));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 1);

        let e = col("boolean").eq(lit(false));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);
        assert_eq!(metrics.num_scanned_files(), metrics.keep_count);
        assert_eq!(metrics.skip_count, 1);

        // Ensure that tables without stats and partition columns can be pruned for just partitions
        // let table = open_table("./tests/data/delta-0.8.0-null-partition").await?;

        /*
        // Logically this should prune. See above
        let e = col("k").eq(lit("A")).and(col("k").is_not_null());
        let metrics = get_scan_metrics(&table, &state, &[e]).await.unwrap();
        println!("{metrics:?}");
        assert!(metrics.num_scanned_files() == 1);

        let e = col("k").eq(lit("B"));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert!(metrics.num_scanned_files() == 1);
        */

        // Check pruning for null partitions
        // let e = col("k").is_null();
        // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        // assert_eq!(metrics.num_scanned_files(), 1);

        // Check pruning for null partitions. Since there are no record count statistics pruning cannot be done
        // let e = col("k").is_not_null();
        // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        // assert_eq!(metrics.num_scanned_files(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_partitioned_types() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/delta-2.2.0-partitioned-types")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx.sql("SELECT * FROM demo").await?.collect().await?;

        let expected = vec![
            "+----+----+----+",
            "| c3 | c1 | c2 |",
            "+----+----+----+",
            "| 5  | 4  | c  |",
            "| 6  | 5  | b  |",
            "| 4  | 6  | a  |",
            "+----+----+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("c3", ArrowDataType::Int32, true),
            ArrowField::new("c1", ArrowDataType::Int32, true),
            ArrowField::new(
                "c2",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt16),
                    Box::new(ArrowDataType::Utf8),
                ),
                true,
            ),
        ]);

        assert_eq!(
            Arc::new(expected_schema),
            Arc::new(
                batches[0]
                    .schema()
                    .as_ref()
                    .clone()
                    .with_metadata(Default::default())
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_scan_timestamps() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/table_with_edge_timestamps")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx.sql("SELECT * FROM demo").await?.collect().await?;

        // Without defining a schema of the select the default for a timestamp is ms UTC
        let expected = vec![
            "+-----------------------------+----------------------+------------+",
            "| BIG_DATE                    | NORMAL_DATE          | SOME_VALUE |",
            "+-----------------------------+----------------------+------------+",
            "| 1816-03-28T05:56:08.066278Z | 2022-02-01T00:00:00Z | 2          |",
            "| 1816-03-29T05:56:08.066278Z | 2022-01-01T00:00:00Z | 1          |",
            "+-----------------------------+----------------------+------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_issue_1292_datafusion_sql_projection() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/http_requests")
            .await
            .unwrap();
        ctx.register_table("http_requests", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT \"ClientRequestURI\" FROM http_requests LIMIT 5")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+------------------+",
            "| ClientRequestURI |",
            "+------------------+",
            "| /                |",
            "| /                |",
            "| /                |",
            "| /                |",
            "| /                |",
            "+------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_issue_1291_datafusion_sql_partitioned_data() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test//tests/data/http_requests")
            .await
            .unwrap();
        ctx.register_table("http_requests", Arc::new(table))?;

        let batches = ctx
        .sql(
            "SELECT \"ClientRequestURI\", date FROM http_requests WHERE date > '2023-04-13' LIMIT 5",
        )
        .await?
        .collect()
        .await?;

        let expected = vec![
            "+------------------+------------+",
            "| ClientRequestURI | date       |",
            "+------------------+------------+",
            "| /                | 2023-04-14 |",
            "| /                | 2023-04-14 |",
            "| /                | 2023-04-14 |",
            "| /                | 2023-04-14 |",
            "| /                | 2023-04-14 |",
            "+------------------+------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
    #[ignore]
    #[tokio::test]
    async fn test_issue_1374() -> Result<()> {
        let ctx = SessionContext::new();
        let table = open_table("../test/tests/data/issue_1374").await.unwrap();
        ctx.register_table("t", Arc::new(table))?;

        let batches = ctx
            .sql(
                r#"SELECT *
        FROM t
        WHERE timestamp BETWEEN '2023-05-24T00:00:00.000Z' AND '2023-05-25T00:00:00.000Z'
        LIMIT 5
        "#,
            )
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+----------------------------+-------------+------------+",
            "| timestamp                  | temperature | date       |",
            "+----------------------------+-------------+------------+",
            "| 2023-05-24T00:01:25.010301 | 8           | 2023-05-24 |",
            "| 2023-05-24T00:01:25.013902 | 21          | 2023-05-24 |",
            "| 2023-05-24T00:01:25.013972 | 58          | 2023-05-24 |",
            "| 2023-05-24T00:01:25.014025 | 24          | 2023-05-24 |",
            "| 2023-05-24T00:01:25.014072 | 90          | 2023-05-24 |",
            "+----------------------------+-------------+------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_issue_1619_parquet_panic_using_map_type() -> Result<()> {
        let _ = tokio::fs::remove_dir_all("./tests/data/issue-1619").await;
        let fields: Vec<StructField> = vec![StructField::new(
            "metadata".to_string(),
            DataType::Map(Box::new(MapType::new(
                DataType::Primitive(PrimitiveType::String),
                DataType::Primitive(PrimitiveType::String),
                true,
            ))),
            true,
        )];
        let schema = StructType::new(fields);
        let table =
            deltalake_core::DeltaTableBuilder::from_uri("./tests/data/issue-1619").build()?;
        let _ = DeltaOps::from(table)
            .create()
            .with_columns(schema.fields().cloned())
            .await?;

        let mut table = open_table("./tests/data/issue-1619").await?;

        let mut writer = JsonWriter::for_table(&table).unwrap();
        writer
            .write(vec![
                serde_json::json!({"metadata": {"hello": "world", "something": null}}),
            ])
            .await
            .unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table))?;

        let batches = ctx.sql(r#"SELECT * FROM t"#).await?.collect().await?;

        let expected = vec![
            "+-----------------------------+",
            "| metadata                    |",
            "+-----------------------------+",
            "| {hello: world, something: } |", // unclear why it doesn't say `null` for something...
            "+-----------------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_issue_2105() -> Result<()> {
        use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path();

        let file_store = LocalFileSystem::new_with_prefix(path).unwrap();
        let log_store = default_logstore(
            Arc::new(file_store),
            Arc::new(LocalFileSystem::new()),
            &Url::from_file_path(path).unwrap(),
            &Default::default(),
        );

        let tbl = CreateBuilder::new()
            .with_log_store(log_store.clone())
            .with_save_mode(SaveMode::Overwrite)
            .with_table_name("test")
            .with_column(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            );
        let tbl = tbl.await.unwrap();
        let ctx = SessionContext::new();
        let plan = Arc::new(
            ctx.sql("SELECT 1 as id")
                .await
                .unwrap()
                .logical_plan()
                .clone(),
        );
        let write_builder = WriteBuilder::new(log_store, tbl.state);
        let _ = write_builder
            .with_input_execution_plan(plan)
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(deltalake_core::operations::write::SchemaMode::Overwrite)
            .await
            .unwrap();

        let table = open_table(path.to_str().unwrap()).await.unwrap();
        let prov: Arc<dyn TableProvider> = Arc::new(table);
        ctx.register_table("test", prov).unwrap();
        let mut batches = ctx
            .sql("SELECT * FROM test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = batches.pop().unwrap();

        let expected_schema = Schema::new(vec![Field::new("id", ArrowDataType::Int64, false)]);
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        Ok(())
    }
}

async fn test_datafusion(context: &IntegrationContext) -> TestResult {
    context.load_table(TestTables::Simple).await?;

    simple_query(context).await?;

    Ok(())
}

async fn simple_query(context: &IntegrationContext) -> TestResult {
    let table_uri = context.uri_for_table(TestTables::Simple);

    let options = "'DYNAMO_LOCK_OWNER_NAME' 's3::deltars/simple'".to_string();

    let sql = format!(
        "CREATE EXTERNAL TABLE demo \
        STORED AS DELTATABLE \
        OPTIONS ({options}) \
        LOCATION '{table_uri}'",
    );

    let ctx = context_with_delta_table_factory();
    let _ = ctx
        .sql(sql.as_str())
        .await
        .expect("Failed to register table!");

    let batches = ctx
        .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int64Array::from(vec![7, 9])).as_ref(),
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_adapter_empty_batch() {
    let ctx = SessionContext::new();
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    // Create table with a single column
    let table = DeltaOps::try_from_uri(table_uri)
        .await
        .unwrap()
        .create()
        .with_column(
            "a",
            DataType::Primitive(PrimitiveType::Integer),
            false,
            None,
        )
        .await
        .unwrap();

    // Write single column
    let a_arr = Int32Array::from(vec![1, 2, 3]);
    let table = DeltaOps(table)
        .write(vec![RecordBatch::try_from_iter_with_nullable(vec![(
            "a",
            Arc::new(a_arr) as ArrayRef,
            false,
        )])
        .unwrap()])
        .await
        .unwrap();

    // Evolve schema by writing a batch with new nullable column
    let a_arr = Int32Array::from(vec![4, 5, 6]);
    let b_arr = Int32Array::from(vec![7, 8, 9]);
    let table = DeltaOps(table)
        .write(vec![RecordBatch::try_from_iter_with_nullable(vec![
            ("a", Arc::new(a_arr) as ArrayRef, false),
            ("b", Arc::new(b_arr) as ArrayRef, true),
        ])
        .unwrap()])
        .with_schema_mode(SchemaMode::Merge)
        .await
        .unwrap();

    // Ensure we can project only the new column which does not exist in files from first write
    let batches = ctx
        .read_table(Arc::new(table))
        .unwrap()
        .select_exprs(&["b"])
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        #[rustfmt::skip]
        &[
            "+---+",
            "| b |",
            "+---+",
            "|   |",
            "|   |",
            "|   |",
            "| 7 |",
            "| 8 |",
            "| 9 |",
            "+---+",
        ],
        &batches
    );
}

mod date_partitions {
    use super::*;

    async fn setup_test(table_uri: &str) -> Result<DeltaTable, Box<dyn Error>> {
        let columns = vec![
            StructField::new(
                "id".to_owned(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
            StructField::new(
                "date".to_owned(),
                DataType::Primitive(PrimitiveType::Date),
                false,
            ),
        ];

        let dt = DeltaOps::try_from_uri(table_uri)
            .await?
            .create()
            .with_columns(columns)
            .with_partition_columns(["date"])
            .await?;

        Ok(dt)
    }

    fn get_batch(ids: Vec<i32>, dates: Vec<i32>) -> Result<RecordBatch, Box<dyn Error>> {
        let ids_array: PrimitiveArray<arrow_array::types::Int32Type> = Int32Array::from(ids);
        let date_array = Date32Array::from(dates);

        Ok(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("date", ArrowDataType::Date32, false),
            ])),
            vec![Arc::new(ids_array), Arc::new(date_array)],
        )?)
    }

    async fn write(
        writer: &mut RecordBatchWriter,
        table: &mut DeltaTable,
        batch: RecordBatch,
    ) -> Result<(), DeltaTableError> {
        writer.write(batch).await?;
        writer.flush_and_commit(table).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_issue_1445_date_partition() -> Result<()> {
        let ctx = SessionContext::new();
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_uri = tmp_dir.path().to_str().to_owned().unwrap();
        let mut dt = setup_test(table_uri).await.unwrap();
        let mut writer = RecordBatchWriter::for_table(&dt)?;
        write(
            &mut writer,
            &mut dt,
            get_batch(vec![2], vec![19517]).unwrap(),
        )
        .await?;
        ctx.register_table("t", Arc::new(dt))?;

        let batches = ctx
            .sql(
                r#"SELECT *
            FROM t
            WHERE date > '2023-06-07'
            "#,
            )
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+----+------------+",
            "| id | date       |",
            "+----+------------+",
            "| 2  | 2023-06-09 |",
            "+----+------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
