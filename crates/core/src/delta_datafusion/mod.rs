//! Datafusion integration for Delta Table
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::execution::context::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake_core::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow_array::types::UInt16Type;
use arrow_array::{
    Array, BooleanArray, DictionaryArray, RecordBatch, StringArray, TypedDictionaryArray,
};
use arrow_cast::display::array_value_to_string;
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    config::ConfigOptions, Column, DFSchema, DataFusionError, Result as DataFusionResult,
    TableReference, ToDFSchema,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    FileSource, ParquetSource,
};
use datafusion::datasource::{listing::PartitionedFile, MemTable, TableProvider, TableType};
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::logical_plan::CreateExternalTable;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    col, BinaryExpr, Expr, Extension, LogicalPlan, Operator, TableProviderFilterPushDown,
    Volatility,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use datafusion::sql::planner::ParserOptions;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use either::Either;
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use url::Url;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{
    Add, DataCheck, EagerSnapshot, Invariant, LogDataHandler, Snapshot, StructTypeExt,
};
use crate::logstore::LogStoreRef;
use crate::table::builder::ensure_table_uri;
use crate::table::state::DeltaTableState;
use crate::table::{Constraint, GeneratedColumn};
use crate::{open_table, open_table_with_storage_options, DeltaTable};

pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

pub mod cdf;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod planner;

pub use cdf::scan::DeltaCdfTableProvider;

mod schema_adapter;

impl From<DeltaTableError> for DataFusionError {
    fn from(err: DeltaTableError) -> Self {
        match err {
            DeltaTableError::Arrow { source } => DataFusionError::from(source),
            DeltaTableError::Io { source } => DataFusionError::IoError(source),
            DeltaTableError::ObjectStore { source } => DataFusionError::from(source),
            DeltaTableError::Parquet { source } => DataFusionError::from(source),
            _ => DataFusionError::External(Box::new(err)),
        }
    }
}

impl From<DataFusionError> for DeltaTableError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::ArrowError(source, _) => DeltaTableError::from(*source),
            DataFusionError::IoError(source) => DeltaTableError::Io { source },
            DataFusionError::ObjectStore(source) => DeltaTableError::from(*source),
            DataFusionError::ParquetError(source) => DeltaTableError::from(*source),
            _ => DeltaTableError::Generic(err.to_string()),
        }
    }
}

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Parse an expression string into a datafusion [`Expr`]
    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr>;
}

impl DataFusionMixins for Snapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        _arrow_schema(self, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        _arrow_schema(self, false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().arrow_schema()
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().input_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        self.snapshot().parse_predicate_expression(expr, df_state)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot.arrow_schema()
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot.input_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        self.snapshot.parse_predicate_expression(expr, df_state)
    }
}

fn _arrow_schema(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    let meta = snapshot.metadata();

    let schema = meta.parse_schema()?;
    let fields = schema
        .fields()
        .filter(|f| !meta.partition_columns().contains(&f.name().to_string()))
        .map(|f| f.try_into_arrow())
        .chain(
            // We need stable order between logical and physical schemas, but the order of
            // partitioning columns is not always the same in the json schema and the array
            meta.partition_columns().iter().map(|partition_col| {
                let f = schema.field(partition_col).unwrap();
                let field: Field = f.try_into_arrow()?;
                let corrected = if wrap_partitions {
                    match field.data_type() {
                        // Only dictionary-encode types that may be large
                        // // https://github.com/apache/arrow-datafusion/pull/5545
                        ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary => {
                            wrap_partition_type_in_dict(field.data_type().clone())
                        }
                        _ => field.data_type().clone(),
                    }
                } else {
                    field.data_type().clone()
                };
                Ok(field.with_data_type(corrected))
            }),
        )
        .collect::<Result<Vec<Field>, _>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

pub(crate) fn files_matching_predicate<'a>(
    snapshot: &'a EagerSnapshot,
    filters: &[Expr],
) -> DeltaResult<impl Iterator<Item = Add> + 'a> {
    if let Some(Some(predicate)) =
        (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
    {
        let expr = SessionContext::new()
            .create_physical_expr(predicate, &snapshot.arrow_schema()?.to_dfschema()?)?;
        let pruning_predicate = PruningPredicate::try_new(expr, snapshot.arrow_schema()?)?;
        Ok(Either::Left(
            snapshot
                .file_actions()?
                .zip(pruning_predicate.prune(snapshot)?)
                .filter_map(
                    |(action, keep_file)| {
                        if keep_file {
                            Some(action)
                        } else {
                            None
                        }
                    },
                ),
        ))
    } else {
        Ok(Either::Right(snapshot.file_actions()?))
    }
}

pub(crate) fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<TypedDictionaryArray<'a, UInt16Type, StringArray>> {
    let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());
    batch
        .column_by_name(path_column)
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .ok_or_else(err)?
        .downcast_dict::<StringArray>()
        .ok_or_else(err)
}

impl DeltaTableState {
    /// Provide table level statistics to Datafusion
    pub fn datafusion_table_statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
pub(crate) fn register_store(store: LogStoreRef, env: Arc<RuntimeEnv>) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub(crate) fn df_logical_schema(
    snapshot: &DeltaTableState,
    file_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let table_partition_cols = snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .unwrap()
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            ArrowDataType::Utf8,
            true,
        )));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    wrap_partition_values: Option<bool>,
    /// Whether to push down filter in end result or just prune the files
    enable_parquet_pushdown: bool,
    /// Schema to scan table with
    schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = Some(wrap);
        self
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            let input_schema = snapshot.input_schema()?;
            let mut column_names: HashSet<&String> = HashSet::new();
            for field in input_schema.fields.iter() {
                column_names.insert(field.name());
            }

            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {name} exits"
                        )));
                    }

                    Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{prefix}_{idx}");
                    }

                    Some(name)
                }
            }
        } else {
            None
        };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values.unwrap_or(true),
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}

pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a DeltaTableState,
    log_store: LogStoreRef,
    filter: Option<Expr>,
    session: &'a dyn Session,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: Option<DeltaScanConfig>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a DeltaTableState,
        log_store: LogStoreRef,
        session: &'a dyn Session,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            log_store,
            filter: None,
            session,
            projection: None,
            limit: None,
            files: None,
            config: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        let config = match self.config {
            Some(config) => config,
            None => DeltaScanConfigBuilder::new().build(self.snapshot)?,
        };

        let schema = match config.schema.clone() {
            Some(value) => Ok(value),
            None => self.snapshot.arrow_schema(),
        }?;

        let logical_schema = df_logical_schema(
            self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            if let Some(expr) = &self.filter {
                for c in expr.column_refs() {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let context = SessionContext::new();
        let df_schema = logical_schema.clone().to_dfschema()?;

        let logical_filter = self
            .filter
            .clone()
            .map(|expr| simplify_expr(&context, &df_schema, expr));
        // only inexact filters should be pushed down to the data source, doing otherwise
        // will make stats inexact and disable datafusion optimizations like AggregateStatistics
        let pushdown_filter = self
            .filter
            .and_then(|expr| {
                let predicates = split_conjunction(&expr);
                let pushdown_filters = get_pushdown_filters(
                    &predicates,
                    self.snapshot.metadata().partition_columns().as_slice(),
                );

                let filtered_predicates = predicates
                    .into_iter()
                    .zip(pushdown_filters.into_iter())
                    .filter_map(|(filter, pushdown)| {
                        if pushdown == TableProviderFilterPushDown::Inexact {
                            Some(filter.clone())
                        } else {
                            None
                        }
                    });
                conjunction(filtered_predicates)
            })
            .map(|expr| simplify_expr(&context, &df_schema, expr));

        // Perform Pruning of files to scan
        let (files, files_scanned, files_pruned, pruning_mask) = match self.files {
            Some(files) => {
                let files = files.to_owned();
                let files_scanned = files.len();
                (files, files_scanned, 0, None)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && self.limit.is_none() {
                    let files = self.snapshot.file_actions()?;
                    let files_scanned = files.len();
                    (files, files_scanned, 0, None)
                } else {
                    let num_containers = self.snapshot.num_containers();

                    let files_to_prune = if let Some(predicate) = &logical_filter {
                        let pruning_predicate =
                            PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                        pruning_predicate.prune(self.snapshot)?
                    } else {
                        vec![true; num_containers]
                    };

                    // needed to enforce limit and deal with missing statistics
                    // rust port of https://github.com/delta-io/delta/pull/1495
                    let mut pruned_without_stats = vec![];
                    let mut rows_collected = 0;
                    let mut files = vec![];

                    for (action, keep) in self
                        .snapshot
                        .file_actions_iter()?
                        .zip(files_to_prune.iter().cloned())
                    {
                        // prune file based on predicate pushdown
                        if keep {
                            // prune file based on limit pushdown
                            if let Some(limit) = self.limit {
                                if let Some(stats) = action.get_stats()? {
                                    if rows_collected <= limit as i64 {
                                        rows_collected += stats.num_records;
                                        files.push(action.to_owned());
                                    } else {
                                        break;
                                    }
                                } else {
                                    // some files are missing stats; skipping but storing them
                                    // in a list in case we can't reach the target limit
                                    pruned_without_stats.push(action.to_owned());
                                }
                            } else {
                                files.push(action.to_owned());
                            }
                        }
                    }

                    if let Some(limit) = self.limit {
                        if rows_collected < limit as i64 {
                            files.extend(pruned_without_stats);
                        }
                    }

                    let files_scanned = files.len();
                    let files_pruned = num_containers - files_scanned;
                    (files, files_scanned, files_pruned, Some(files_to_prune))
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                let partition_value = if config.wrap_partition_values {
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(action.path.clone())))
                } else {
                    ScalarValue::Utf8(Some(action.path.clone()))
                };
                part.partition_values.push(partition_value);
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<arrow::datatypes::FieldRef>>(),
        ));

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        if let Some(file_column_name) = &config.file_column_name {
            let field_name_datatype = if config.wrap_partition_values {
                wrap_partition_type_in_dict(ArrowDataType::Utf8)
            } else {
                ArrowDataType::Utf8
            };
            table_partition_cols.push(Field::new(
                file_column_name.clone(),
                field_name_datatype,
                false,
            ));
        }

        // FIXME - where is the correct place to marry file pruning with statistics pruning?
        //  Temporarily re-generating the log handler, just so that we can compute the stats.
        //  Should we update datafusion_table_statistics to optionally take the mask?
        let stats = if let Some(mask) = pruning_mask {
            let es = self.snapshot.snapshot();
            let pruned_stats = prune_file_statistics(&es.files, mask);
            LogDataHandler::new(&pruned_stats, es.metadata(), es.schema()).statistics()
        } else {
            self.snapshot.datafusion_table_statistics()
        };

        let stats = stats.unwrap_or(Statistics::new_unknown(&schema));

        let parquet_options = TableParquetOptions {
            global: self.session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut file_source = ParquetSource::new(parquet_options);

        // Sometimes (i.e Merge) we want to prune files that don't make the
        // filter and read the entire contents for files that do match the
        // filter
        if let Some(predicate) = pushdown_filter {
            if config.enable_parquet_pushdown {
                file_source = file_source.with_predicate(predicate);
            }
        };
        let file_source =
            file_source.with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))?;

        let file_scan_config =
            FileScanConfigBuilder::new(self.log_store.object_store_url(), file_schema, file_source)
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![FileGroup::from(vec![])]
                    } else {
                        file_groups.into_values().map(FileGroup::from).collect()
                    },
                )
                .with_statistics(stats)
                .with_projection(self.projection.cloned())
                .with_limit(self.limit)
                .with_table_partition_cols(table_partition_cols)
                .build();

        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("files_scanned")
            .add(files_scanned);
        MetricBuilder::new(&metrics)
            .global_counter("files_pruned")
            .add(files_pruned);

        Ok(DeltaScan {
            table_uri: ensure_table_uri(self.log_store.root_uri())?.as_str().into(),
            parquet_scan: DataSourceExec::from_data_source(file_scan_config),
            config,
            logical_schema,
            metrics,
        })
    }
}

fn simplify_expr(
    context: &SessionContext,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    // Simplify the expression first
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    let simplified = simplifier.simplify(expr).unwrap();

    context.create_physical_expr(simplified, df_schema).unwrap()
}

fn prune_file_statistics(
    record_batches: &Vec<RecordBatch>,
    pruning_mask: Vec<bool>,
) -> Vec<RecordBatch> {
    let mut filtered_batches = Vec::new();
    let mut mask_offset = 0;

    for batch in record_batches {
        let num_rows = batch.num_rows();
        let batch_mask = &pruning_mask[mask_offset..mask_offset + num_rows];
        mask_offset += num_rows;

        let boolean_mask = BooleanArray::from(batch_mask.to_vec());
        let filtered_batch =
            filter_record_batch(batch, &boolean_mask).expect("Failed to filter RecordBatch");
        filtered_batches.push(filtered_batch);
    }

    filtered_batches
}

// TODO: implement this for Snapshot, not for DeltaTable
#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.snapshot().unwrap().arrow_schema().unwrap()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        register_store(self.log_store(), session.runtime_env().clone());
        let filter_expr = conjunction(filters.iter().cloned());

        let scan = DeltaScanBuilder::new(self.snapshot()?, self.log_store(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .build()
            .await?;

        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot()?.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot().ok()?.datafusion_table_statistics()
    }
}

fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .cloned()
        .map(|expr| {
            let applicable = expr_is_exact_predicate_for_cols(partition_cols, expr);
            if !expr.column_refs().is_empty() && applicable {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

// inspired from datafusion::listing::helpers, but adapted to only stats based pruning
fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(|expr| match expr {
        Expr::Column(Column { ref name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            // TODO: decide if we should constrain this to Utf8 columns (including views, dicts etc)

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { ref op, .. }) => {
            is_applicable &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_applicable {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    })
    .unwrap();
    is_applicable
}

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Vec<Add>>,
}

impl DeltaTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(&snapshot, &config.file_column_name, config.schema.clone())?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    /// Define which files to consider while building a scan, for advanced usecases
    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(files);
        self
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        register_store(self.log_store.clone(), session.runtime_env().clone());
        let filter_expr = conjunction(filters.iter().cloned());

        let mut scan = DeltaScanBuilder::new(&self.snapshot, self.log_store.clone(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .with_scan_config(self.config.clone());

        if let Some(files) = &self.files {
            scan = scan.with_files(files);
        }
        Ok(Arc::new(scan.build().await?))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics()
    }
}

// TODO: this will likely also need to perform column mapping later when we support reader protocol v2
/// A wrapper for parquet scans
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub table_uri: String,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<ArrowSchema>,
    /// Metrics for scan reported via DataFusion
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeltaScanWire {
    pub table_uri: String,
    pub config: DeltaScanConfig,
    pub logical_schema: Arc<ArrowSchema>,
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
}

impl ExecutionPlan for DeltaScan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(DeltaScan {
            table_uri: self.table_uri.clone(),
            config: self.config.clone(),
            parquet_scan: children[0].clone(),
            logical_schema: self.logical_schema.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.parquet_scan.partition_statistics(partition)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(parquet_scan) = self.parquet_scan.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(DeltaScan {
                table_uri: self.table_uri.clone(),
                config: self.config.clone(),
                parquet_scan,
                logical_schema: self.logical_schema.clone(),
                metrics: self.metrics.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub(crate) fn get_null_of_arrow_type(t: &ArrowDataType) -> DeltaResult<ScalarValue> {
    match t {
        ArrowDataType::Null => Ok(ScalarValue::Null),
        ArrowDataType::Boolean => Ok(ScalarValue::Boolean(None)),
        ArrowDataType::Int8 => Ok(ScalarValue::Int8(None)),
        ArrowDataType::Int16 => Ok(ScalarValue::Int16(None)),
        ArrowDataType::Int32 => Ok(ScalarValue::Int32(None)),
        ArrowDataType::Int64 => Ok(ScalarValue::Int64(None)),
        ArrowDataType::UInt8 => Ok(ScalarValue::UInt8(None)),
        ArrowDataType::UInt16 => Ok(ScalarValue::UInt16(None)),
        ArrowDataType::UInt32 => Ok(ScalarValue::UInt32(None)),
        ArrowDataType::UInt64 => Ok(ScalarValue::UInt64(None)),
        ArrowDataType::Float32 => Ok(ScalarValue::Float32(None)),
        ArrowDataType::Float64 => Ok(ScalarValue::Float64(None)),
        ArrowDataType::Date32 => Ok(ScalarValue::Date32(None)),
        ArrowDataType::Date64 => Ok(ScalarValue::Date64(None)),
        ArrowDataType::Binary => Ok(ScalarValue::Binary(None)),
        ArrowDataType::FixedSizeBinary(size) => {
            Ok(ScalarValue::FixedSizeBinary(size.to_owned(), None))
        }
        ArrowDataType::LargeBinary => Ok(ScalarValue::LargeBinary(None)),
        ArrowDataType::Utf8 => Ok(ScalarValue::Utf8(None)),
        ArrowDataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(None)),
        ArrowDataType::Decimal128(precision, scale) => Ok(ScalarValue::Decimal128(
            None,
            precision.to_owned(),
            scale.to_owned(),
        )),
        ArrowDataType::Timestamp(unit, tz) => {
            let tz = tz.to_owned();
            Ok(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(None, tz),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(None, tz),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(None, tz),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(None, tz),
            })
        }
        ArrowDataType::Dictionary(k, v) => Ok(ScalarValue::Dictionary(
            k.clone(),
            Box::new(get_null_of_arrow_type(v).unwrap()),
        )),
        //Unsupported types...
        ArrowDataType::Float16
        | ArrowDataType::Decimal256(_, _)
        | ArrowDataType::Union(_, _)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::Struct(_)
        | ArrowDataType::List(_)
        | ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::RunEndEncoded(_, _)
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View
        | ArrowDataType::LargeListView(_)
        | ArrowDataType::ListView(_)
        | ArrowDataType::Map(_, _) => Err(DeltaTableError::Generic(format!(
            "Unsupported data type for Delta Lake {t}"
        ))),
    }
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                            .unwrap_or(Some(ScalarValue::Null))
                            .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = Utc.from_utc_datetime(
        &DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .unwrap()
            .naive_utc(),
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action.try_into().unwrap()
        },
        partition_values,
        range: None,
        extensions: None,
        statistics: None,
        metadata_size_hint: None,
    }
}

fn parse_date(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(string, &ArrowDataType::Date32)?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

fn parse_timestamp(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(
        string,
        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<Option<ScalarValue>> {
    match stat_val {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => Ok(Some(get_null_of_arrow_type(field_dt)?)),
        serde_json::Value::String(string_val) => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                string_val.to_owned(),
                field_dt,
            )?)),
        },
        other => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                other.to_string(),
                field_dt,
            )?)),
        },
    }
}

pub(crate) async fn execute_plan_to_batch(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
) -> DeltaResult<arrow::record_batch::RecordBatch> {
    let data = futures::future::try_join_all(
        (0..plan.properties().output_partitioning().partition_count()).map(|p| {
            let plan_copy = plan.clone();
            let task_context = state.task_ctx().clone();
            async move {
                let batch_stream = plan_copy.execute(p, task_context)?;

                let schema = batch_stream.schema();

                let batches = batch_stream.try_collect::<Vec<_>>().await?;

                DataFusionResult::<_>::Ok(concat_batches(&schema, batches.iter())?)
            }
        }),
    )
    .await?;

    Ok(concat_batches(&plan.schema(), data.iter())?)
}

/// Responsible for checking batches of data conform to table's invariants, constraints and nullability.
#[derive(Clone, Default)]
pub struct DeltaDataChecker {
    constraints: Vec<Constraint>,
    invariants: Vec<Invariant>,
    generated_columns: Vec<GeneratedColumn>,
    non_nullable_columns: Vec<String>,
    ctx: SessionContext,
}

impl DeltaDataChecker {
    /// Create a new DeltaDataChecker with no invariants or constraints
    pub fn empty() -> Self {
        Self {
            invariants: vec![],
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of invariants
    pub fn new_with_invariants(invariants: Vec<Invariant>) -> Self {
        Self {
            invariants,
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of constraints
    pub fn new_with_constraints(constraints: Vec<Constraint>) -> Self {
        Self {
            constraints,
            invariants: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of generated columns
    pub fn new_with_generated_columns(generated_columns: Vec<GeneratedColumn>) -> Self {
        Self {
            constraints: vec![],
            invariants: vec![],
            generated_columns,
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Specify the Datafusion context
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        self.ctx = context;
        self
    }

    /// Add the specified set of constraints to the current DeltaDataChecker's constraints
    pub fn with_extra_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        self.constraints.extend(constraints);
        self
    }

    /// Create a new DeltaDataChecker
    pub fn new(snapshot: &DeltaTableState) -> Self {
        let invariants = snapshot.schema().get_invariants().unwrap_or_default();
        let generated_columns = snapshot
            .schema()
            .get_generated_columns()
            .unwrap_or_default();
        let constraints = snapshot.table_config().get_constraints();
        let non_nullable_columns = snapshot
            .schema()
            .fields()
            .filter_map(|f| {
                if !f.is_nullable() {
                    Some(f.name().clone())
                } else {
                    None
                }
            })
            .collect_vec();
        Self {
            invariants,
            constraints,
            generated_columns,
            non_nullable_columns,
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Check that a record batch conforms to table's invariants.
    ///
    /// If it does not, it will return [DeltaTableError::InvalidData] with a list
    /// of values that violated each invariant.
    pub async fn check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.check_nullability(record_batch)?;
        self.enforce_checks(record_batch, &self.invariants).await?;
        self.enforce_checks(record_batch, &self.constraints).await?;
        self.enforce_checks(record_batch, &self.generated_columns)
            .await
    }

    /// Return true if all the nullability checks are valid
    fn check_nullability(&self, record_batch: &RecordBatch) -> Result<bool, DeltaTableError> {
        let mut violations = Vec::new();
        for col in self.non_nullable_columns.iter() {
            if let Some(arr) = record_batch.column_by_name(col) {
                if arr.null_count() > 0 {
                    violations.push(format!(
                        "Non-nullable column violation for {col}, found {} null values",
                        arr.null_count()
                    ));
                }
            } else {
                violations.push(format!(
                    "Non-nullable column violation for {col}, not found in batch!"
                ));
            }
        }
        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(true)
        }
    }

    async fn enforce_checks<C: DataCheck>(
        &self,
        record_batch: &RecordBatch,
        checks: &[C],
    ) -> Result<(), DeltaTableError> {
        if checks.is_empty() {
            return Ok(());
        }
        let table = MemTable::try_new(record_batch.schema(), vec![vec![record_batch.clone()]])?;
        table.schema();
        // Use a random table name to avoid clashes when running multiple parallel tasks, e.g. when using a partitioned table
        let table_name: String = uuid::Uuid::new_v4().to_string();
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let mut violations: Vec<String> = Vec::new();

        for check in checks {
            if check.get_name().contains('.') {
                return Err(DeltaTableError::Generic(
                    "delta constraints for nested columns are not supported at the moment."
                        .to_string(),
                ));
            }

            let field_to_select = if check.as_any().is::<Constraint>() {
                "*"
            } else {
                check.get_name()
            };
            let sql = format!(
                "SELECT {} FROM `{table_name}` WHERE NOT ({}) LIMIT 1",
                field_to_select,
                check.get_expression()
            );

            let dfs: Vec<RecordBatch> = self.ctx.sql(&sql).await?.collect().await?;
            if !dfs.is_empty() && dfs[0].num_rows() > 0 {
                let value: String = dfs[0]
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, 0).unwrap_or(String::from("null")))
                    .join(", ");

                let msg = format!(
                    "Check or Invariant ({}) violated by value in row: [{value}]",
                    check.get_expression(),
                );
                violations.push(msg);
            }
        }

        self.ctx.deregister_table(&table_name)?;
        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(())
        }
    }
}

/// A codec for deltalake physical plans
#[derive(Debug)]
pub struct DeltaPhysicalCodec {}

impl PhysicalExtensionCodec for DeltaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let wire: DeltaScanWire = serde_json::from_reader(buf)
            .map_err(|_| DataFusionError::Internal("Unable to decode DeltaScan".to_string()))?;
        let delta_scan = DeltaScan {
            table_uri: wire.table_uri,
            parquet_scan: (*inputs)[0].clone(),
            config: wire.config,
            logical_schema: wire.logical_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        };
        Ok(Arc::new(delta_scan))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let delta_scan = node
            .as_any()
            .downcast_ref::<DeltaScan>()
            .ok_or_else(|| DataFusionError::Internal("Not a delta scan!".to_string()))?;

        let wire = DeltaScanWire {
            table_uri: delta_scan.table_uri.to_owned(),
            config: delta_scan.config.clone(),
            logical_schema: delta_scan.logical_schema.clone(),
        };
        serde_json::to_writer(buf, &wire)
            .map_err(|_| DataFusionError::Internal("Unable to encode delta scan!".to_string()))?;
        Ok(())
    }
}

/// Does serde on DeltaTables
#[derive(Debug)]
pub struct DeltaLogicalCodec {}

impl LogicalExtensionCodec for DeltaLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let provider: DeltaTable = serde_json::from_slice(buf)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))?;
        Ok(Arc::new(provider))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let table = node
            .as_ref()
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| {
                DataFusionError::Internal("Can't encode non-delta tables".to_string())
            })?;
        serde_json::to_writer(buf, table)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))
    }
}

/// Responsible for creating deltatables
#[derive(Debug)]
pub struct DeltaTableFactory {}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = if cmd.options.is_empty() {
            open_table(cmd.to_owned().location).await?
        } else {
            open_table_with_storage_options(cmd.to_owned().location, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}

pub(crate) struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,

    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor<'_> for FindFilesExprProperties {
    type Node = Expr;

    fn f_down(&mut self, expr: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        // TODO: We can likely relax the volatility to STABLE. Would require further
        // research to confirm the same value is generated during the scan and
        // rewrite phases.

        match expr {
            Expr::Column(c) => {
                if !self.partition_columns.contains(&c.name) {
                    self.partition_only = false;
                }
            }
            Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::InList { .. }
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => (),
            Expr::ScalarFunction(scalar_function) => {
                match scalar_function.func.signature().volatility {
                    Volatility::Immutable => (),
                    _ => {
                        self.result = Err(DeltaTableError::Generic(format!(
                            "Find files predicate contains nondeterministic function {}",
                            scalar_function.func.name()
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Find files predicate contains unsupported expression {expr}"
                )));
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
/// Representing the result of the [find_files] function.
pub struct FindFiles {
    /// A list of `Add` objects that match the given predicate
    pub candidates: Vec<Add>,
    /// Was a physical read to the datastore required to determine the candidates
    pub partition_scan: bool,
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    // Given RecordBatches that contains `__delta_rs_path` perform a hash join
    // with actions to obtain original add actions

    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());

        let iter: Box<dyn Iterator<Item = Option<&str>>> = if dict_array {
            let array = get_path_column(&batch, path_column)?;
            Box::new(array.into_iter())
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Box::new(array.into_iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{path_column} cannot be null"
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}

/// Determine which files contain a record that satisfies the predicate
pub(crate) async fn find_files_scan(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = snapshot
        .file_actions_iter()?
        .map(|add| (add.path.clone(), add.to_owned()))
        .collect();

    let scan_config = DeltaScanConfigBuilder {
        include_file_column: true,
        ..Default::default()
    }
    .build(snapshot)?;

    let logical_schema = df_logical_schema(snapshot, &scan_config.file_column_name, None)?;

    // Identify which columns we need to project
    let mut used_columns = expression
        .column_refs()
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .collect::<Result<Vec<usize>, ArrowError>>()?;
    // Add path column
    used_columns.push(logical_schema.index_of(scan_config.file_column_name.as_ref().unwrap())?);

    let scan = DeltaScanBuilder::new(snapshot, log_store, state)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    let config = &scan.config;
    let input_schema = scan.logical_schema.as_ref().to_owned();
    let input_dfschema = input_schema.clone().try_into()?;

    let predicate_expr =
        state.create_physical_expr(Expr::IsTrue(Box::new(expression.clone())), &input_dfschema)?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let task_ctx = Arc::new(TaskContext::from(state));
    let path_batches = datafusion::physical_plan::collect(limit, task_ctx).await?;

    join_batches_with_add_actions(
        path_batches,
        candidate_map,
        config.file_column_name.as_ref().unwrap(),
        true,
    )
}

pub(crate) async fn scan_memory_table(
    snapshot: &DeltaTableState,
    predicate: &Expr,
) -> DeltaResult<Vec<Add>> {
    let actions = snapshot.file_actions()?;

    let batch = snapshot.add_actions_table(true)?;
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    let schema = batch.schema();

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::Generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, ArrowDataType::Utf8, false));

    for field in schema.fields() {
        if field.name().starts_with("partition.") {
            let name = field.name().strip_prefix("partition.").unwrap();

            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(Field::new(
                name,
                field.data_type().to_owned(),
                field.is_nullable(),
            ));
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let batches = df.collect().await?;

    let map = actions
        .into_iter()
        .map(|action| (action.path.clone(), action))
        .collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(batches, map, PATH_COLUMN, false)
}

/// Finds files in a snapshot that match the provided predicate.
pub async fn find_files(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot.metadata();

    match &predicate {
        Some(predicate) => {
            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns().clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let candidates = scan_memory_table(snapshot, predicate).await?;
                Ok(FindFiles {
                    candidates,
                    partition_scan: true,
                })
            } else {
                let candidates =
                    find_files_scan(snapshot, log_store, state, predicate.to_owned()).await?;

                Ok(FindFiles {
                    candidates,
                    partition_scan: false,
                })
            }
        }
        None => Ok(FindFiles {
            candidates: snapshot.file_actions()?,
            partition_scan: true,
        }),
    }
}

/// A wrapper for sql_parser's ParserOptions to capture sane default table defaults
pub struct DeltaParserOptions {
    inner: ParserOptions,
}

impl Default for DeltaParserOptions {
    fn default() -> Self {
        DeltaParserOptions {
            inner: ParserOptions {
                enable_ident_normalization: false,
                ..ParserOptions::default()
            },
        }
    }
}

impl From<DeltaParserOptions> for ParserOptions {
    fn from(value: DeltaParserOptions) -> Self {
        value.inner
    }
}

/// A wrapper for Deltafusion's SessionConfig to capture sane default table defaults
pub struct DeltaSessionConfig {
    inner: SessionConfig,
}

impl Default for DeltaSessionConfig {
    fn default() -> Self {
        DeltaSessionConfig {
            inner: SessionConfig::default()
                .set_bool("datafusion.sql_parser.enable_ident_normalization", false),
        }
    }
}

impl From<DeltaSessionConfig> for SessionConfig {
    fn from(value: DeltaSessionConfig) -> Self {
        value.inner
    }
}

/// A wrapper for Deltafusion's SessionContext to capture sane default table defaults
pub struct DeltaSessionContext {
    inner: SessionContext,
}

impl Default for DeltaSessionContext {
    fn default() -> Self {
        DeltaSessionContext {
            inner: SessionContext::new_with_config(DeltaSessionConfig::default().into()),
        }
    }
}

impl From<DeltaSessionContext> for SessionContext {
    fn from(value: DeltaSessionContext) -> Self {
        value.inner
    }
}

/// A wrapper for Deltafusion's Column to preserve case-sensitivity during string conversion
pub struct DeltaColumn {
    inner: Column,
}

impl From<&str> for DeltaColumn {
    fn from(c: &str) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

/// Create a column, cloning the string
impl From<&String> for DeltaColumn {
    fn from(c: &String) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

/// Create a column, reusing the existing string
impl From<String> for DeltaColumn {
    fn from(c: String) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

impl From<DeltaColumn> for Column {
    fn from(value: DeltaColumn) -> Self {
        value.inner
    }
}

/// Create a column, resuing the existing datafusion column
impl From<Column> for DeltaColumn {
    fn from(c: Column) -> Self {
        DeltaColumn { inner: c }
    }
}

#[cfg(test)]
mod tests {
    use crate::logstore::default_logstore::DefaultLogStore;
    use crate::logstore::ObjectStoreRef;
    use crate::operations::write::SchemaMode;
    use crate::writer::test_utils::get_delta_schema;
    use arrow::array::StructArray;
    use arrow::datatypes::{Field, Schema};
    use arrow_array::cast::AsArray;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::lit;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{visit_execution_plan, ExecutionPlanVisitor, PhysicalExpr};
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf;
    use delta_kernel::path::{LogPathFileType, ParsedLogPath};
    use futures::{stream::BoxStream, StreamExt};
    use object_store::{
        path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };
    use serde_json::json;
    use std::fmt::{Debug, Display, Formatter};
    use std::ops::{Deref, Range};
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

    use super::*;

    // test deserialization of serialized partition values.
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    #[test]
    fn test_to_correct_scalar_value() {
        let reference_pairs = &[
            (
                json!("2015"),
                ArrowDataType::Int16,
                ScalarValue::Int16(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Int32,
                ScalarValue::Int32(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Int64,
                ScalarValue::Int64(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Float32,
                ScalarValue::Float32(Some(2015_f32)),
            ),
            (
                json!("2015"),
                ArrowDataType::Float64,
                ScalarValue::Float64(Some(2015_f64)),
            ),
            (
                json!(2015),
                ArrowDataType::Float64,
                ScalarValue::Float64(Some(2015_f64)),
            ),
            (
                json!("2015-01-01"),
                ArrowDataType::Date32,
                ScalarValue::Date32(Some(16436)),
            ),
            // (
            //     json!("2015-01-01"),
            //     ArrowDataType::Date64,
            //     ScalarValue::Date64(Some(16436)),
            // ),
            // TODO(roeap) there seem to be differences in how precisions are handled locally and in CI, need to investigate
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            //     ScalarValue::TimestampNanosecond(Some(1599565349000000000), None),
            // ),
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            //     ScalarValue::TimestampMicrosecond(Some(1599565349000000), None),
            // ),
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            //     ScalarValue::TimestampMillisecond(Some(1599565349000), None),
            // ),
            (
                json!(true),
                ArrowDataType::Boolean,
                ScalarValue::Boolean(Some(true)),
            ),
        ];

        for (raw, data_type, ref_scalar) in reference_pairs {
            let scalar = to_correct_scalar_value(raw, data_type).unwrap().unwrap();
            assert_eq!(*ref_scalar, scalar)
        }
    }

    #[test]
    fn test_partitioned_file_from_action() {
        let mut partition_values = std::collections::HashMap::new();
        partition_values.insert("month".to_string(), Some("1".to_string()));
        partition_values.insert("year".to_string(), Some("2015".to_string()));
        let action = Add {
            path: "year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string(),
            size: 10644,
            partition_values,
            modification_time: 1660497727833,
            data_change: true,
            stats: None,
            deletion_vector: None,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };
        let schema = ArrowSchema::new(vec![
            Field::new("year", ArrowDataType::Int64, true),
            Field::new("month", ArrowDataType::Int64, true),
        ]);

        let part_columns = vec!["year".to_string(), "month".to_string()];
        let file = partitioned_file_from_action(&action, &part_columns, &schema);
        let ref_file = PartitionedFile {
            object_meta: object_store::ObjectMeta {
                location: Path::from("year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string()),
                last_modified: Utc.timestamp_millis_opt(1660497727833).unwrap(),
                size: 10644,
                e_tag: None,
                version: None,
            },
            partition_values: [ScalarValue::Int64(Some(2015)), ScalarValue::Int64(Some(1))].to_vec(),
            range: None,
            extensions: None,
            statistics: None,
            metadata_size_hint: None,
        };
        assert_eq!(file.partition_values, ref_file.partition_values)
    }

    #[tokio::test]
    async fn test_enforce_invariants() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty invariants is okay
        let invariants: Vec<Invariant> = vec![];
        assert!(DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Valid invariants return Ok(())
        let invariants = vec![
            Invariant::new("a", "a is not null"),
            Invariant::new("b", "b < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let invariants = vec![
            Invariant::new("a", "a is null"),
            Invariant::new("b", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant invariants return a different error
        let invariants = vec![Invariant::new("c", "c > 2000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());

        // Nested invariants are unsupported
        let struct_fields = schema.fields().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            ArrowDataType::Struct(struct_fields),
            false,
        )]));
        let inner = Arc::new(StructArray::from(batch));
        let batch = RecordBatch::try_new(schema, vec![inner]).unwrap();

        let invariants = vec![Invariant::new("x.b", "x.b < 1000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::Generic { .. })));
    }

    #[tokio::test]
    async fn test_enforce_constraints() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty constraints is okay
        let constraints: Vec<Constraint> = vec![];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom_a", "a is not null"),
            Constraint::new("custom_b", "b < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
    }

    /// Ensure that constraints when there are spaces in the field name still work
    ///
    /// See <https://github.com/delta-io/delta-rs/pull/3374>
    #[tokio::test]
    async fn test_constraints_with_spacey_fields() -> DeltaResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b bop", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b bop", "c", "d",
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom a", "a is not null"),
            Constraint::new("custom_b", "`b bop` < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "\"b bop\" < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn roundtrip_test_delta_exec_plan() {
        let ctx = SessionContext::new();
        let codec = DeltaPhysicalCodec {};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let exec_plan = Arc::from(DeltaScan {
            table_uri: "s3://my_bucket/this/is/some/path".to_string(),
            parquet_scan: Arc::from(EmptyExec::new(schema.clone())),
            config: DeltaScanConfig::default(),
            logical_schema: schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        });
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
                .expect("to proto");

        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.deref(), &codec)
            .expect("from proto");
        assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
    }

    #[tokio::test]
    async fn delta_table_provider_with_config() {
        let table = crate::open_table("../test/tests/data/delta-2.2.0-partitioned-types")
            .await
            .unwrap();
        let config = DeltaScanConfigBuilder::new()
            .with_file_column_name(&"file_source")
            .build(table.snapshot().unwrap())
            .unwrap();

        let log_store = table.log_store();
        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log_store, config)
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("select * from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec! [
                "+----+----+----+-------------------------------------------------------------------------------+",
                "| c3 | c1 | c2 | file_source                                                                   |",
                "+----+----+----+-------------------------------------------------------------------------------+",
                "| 4  | 6  | a  | c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet |",
                "| 5  | 4  | c  | c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet |",
                "| 6  | 5  | b  | c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet |",
                "+----+----+----+-------------------------------------------------------------------------------+",
            ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_mixed_partition_order() {
        // Tests issue (1787) where partition columns were incorrect when they
        // have a different order in the metadata and table schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("modified", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new("value", ArrowDataType::Int32, true),
        ]));

        let table = crate::DeltaOps::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )
        .unwrap();
        // write some data
        let table = crate::DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();

        let log_store = table.log_store();
        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log_store, config)
                .unwrap();
        let logical_schema = provider.schema();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let expected_logical_order = vec!["value", "modified", "id"];
        let actual_order: Vec<String> = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        let df = ctx.sql("select * from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+------------+----+",
            "| value | modified   | id |",
            "+-------+------------+----+",
            "| 1     | 2021-02-01 | A  |",
            "| 10    | 2021-02-01 | B  |",
            "| 100   | 2021-02-02 | D  |",
            "| 20    | 2021-02-02 | C  |",
            "+-------+------------+----+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
        assert_eq!(expected_logical_order, actual_order);
    }

    #[tokio::test]
    async fn delta_scan_case_sensitive() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("moDified", ArrowDataType::Utf8, true),
            Field::new("ID", ArrowDataType::Utf8, true),
            Field::new("vaLue", ArrowDataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )
        .unwrap();
        // write some data
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log, config).unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select ID, moDified, vaLue from test")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+----+------------+-------+",
            "| ID | moDified   | vaLue |",
            "+----+------------+-------+",
            "| A  | 2021-02-01 | 1     |",
            "| B  | 2021-02-01 | 10    |",
            "| C  | 2021-02-02 | 20    |",
            "| D  | 2021-02-02 | 100   |",
            "+----+------------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);

        /* TODO: Datafusion doesn't have any options to prevent case-sensitivity with the col func */
        /*
        let df = ctx
            .table("test")
            .await
            .unwrap()
            .select(vec![col("ID"), col("moDified"), col("vaLue")])
            .unwrap();
        let actual = df.collect().await.unwrap();
        assert_batches_sorted_eq!(&expected, &actual);
        */
    }

    #[tokio::test]
    async fn delta_scan_supports_missing_columns() {
        let schema1 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Utf8,
            true,
        )]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("A"),
                Some("B"),
            ]))],
        )
        .unwrap();

        let schema2 = Arc::new(ArrowSchema::new(vec![
            Field::new("col_1", ArrowDataType::Utf8, true),
            Field::new("col_2", ArrowDataType::Utf8, true),
        ]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("E"),
                    Some("F"),
                    Some("G"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("E2"),
                    Some("F2"),
                    Some("G2"),
                ])),
            ],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch2])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let table = crate::DeltaOps(table)
            .write(vec![batch1])
            .with_schema_mode(SchemaMode::Merge)
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log, config).unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("select col_1, col_2 from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| A     |       |",
            "| B     |       |",
            "| E     | E2    |",
            "| F     | F2    |",
            "| G     | G2    |",
            "+-------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_supports_pushdown() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col_1", ArrowDataType::Utf8, false),
            Field::new("col_2", ArrowDataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("A"),
                    Some("B"),
                    Some("C"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("A2"),
                    Some("B2"),
                    Some("C2"),
                ])),
            ],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log, config).unwrap();

        let mut cfg = SessionConfig::default();
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select col_1, col_2 from test WHERE col_1 = 'A'")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| A     | A2    |",
            "+-------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_supports_nested_missing_columns() {
        let column1_schema1: arrow::datatypes::Fields =
            vec![Field::new("col_1a", ArrowDataType::Utf8, true)].into();
        let schema1 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Struct(column1_schema1.clone()),
            true,
        )]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(StructArray::new(
                column1_schema1,
                vec![Arc::new(arrow::array::StringArray::from(vec![
                    Some("A"),
                    Some("B"),
                ]))],
                None,
            ))],
        )
        .unwrap();

        let column1_schema2: arrow_schema::Fields = vec![
            Field::new("col_1a", ArrowDataType::Utf8, true),
            Field::new("col_1b", ArrowDataType::Utf8, true),
        ]
        .into();
        let schema2 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Struct(column1_schema2.clone()),
            true,
        )]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(StructArray::new(
                column1_schema2,
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![
                        Some("E"),
                        Some("F"),
                        Some("G"),
                    ])),
                    Arc::new(arrow::array::StringArray::from(vec![
                        Some("E2"),
                        Some("F2"),
                        Some("G2"),
                    ])),
                ],
                None,
            ))],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch1])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let table = crate::DeltaOps(table)
            .write(vec![batch2])
            .with_schema_mode(SchemaMode::Merge)
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().clone(), log, config).unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select col_1.col_1a, col_1.col_1b from test")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------+--------------------+",
            "| test.col_1[col_1a] | test.col_1[col_1b] |",
            "+--------------------+--------------------+",
            "| A                  |                    |",
            "| B                  |                    |",
            "| E                  | E2                 |",
            "| F                  | F2                 |",
            "| G                  | G2                 |",
            "+--------------------+--------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_multiple_predicate_pushdown() {
        use crate::datafusion::prelude::SessionContext;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("moDified", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new("vaLue", ArrowDataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )
        .unwrap();
        // write some data
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let datafusion = SessionContext::new();
        let table = Arc::new(table);

        datafusion.register_table("snapshot", table).unwrap();

        let df = datafusion
            .sql("select * from snapshot where id > 10000 and id < 20000")
            .await
            .unwrap();

        df.collect().await.unwrap();
    }

    #[tokio::test]
    async fn test_delta_scan_builder_no_scan_config() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let scan = DeltaScanBuilder::new(table.snapshot().unwrap(), table.log_store(), &state)
            .with_filter(Some(col("a").eq(lit("s"))))
            .build()
            .await
            .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert_eq!(visitor.predicate.unwrap().to_string(), "a@0 = s");
    }

    #[tokio::test]
    async fn test_delta_scan_builder_scan_config_disable_pushdown() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let snapshot = table.snapshot().unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let scan = DeltaScanBuilder::new(snapshot, table.log_store(), &state)
            .with_filter(Some(col("a").eq(lit("s"))))
            .with_scan_config(
                DeltaScanConfigBuilder::new()
                    .with_parquet_pushdown(false)
                    .build(snapshot)
                    .unwrap(),
            )
            .build()
            .await
            .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert!(visitor.predicate.is_none());
    }

    #[tokio::test]
    async fn test_delta_scan_applies_parquet_options() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let snapshot = table.snapshot().unwrap();

        let mut config = SessionConfig::default();
        config.options_mut().execution.parquet.pushdown_filters = true;
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();

        let scan = DeltaScanBuilder::new(snapshot, table.log_store(), &state)
            .build()
            .await
            .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert_eq!(ctx.copied_table_options().parquet, visitor.options.unwrap());
    }

    /// Extracts fields from the parquet scan
    #[derive(Default)]
    struct ParquetVisitor {
        predicate: Option<Arc<dyn PhysicalExpr>>,
        options: Option<TableParquetOptions>,
    }

    impl ExecutionPlanVisitor for ParquetVisitor {
        type Error = DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(true);
            };

            let Some(scan_config) = datasource_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };

            if let Some(parquet_source) = scan_config
                .file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
            {
                self.options = Some(parquet_source.table_parquet_options().clone());
                self.predicate = parquet_source.predicate().cloned();
            }

            Ok(true)
        }
    }

    #[tokio::test]
    async fn passes_sanity_checker_when_all_files_filtered() {
        // Run a query that filters out all files and sorts.
        // Verify that it returns an empty set of rows without panicking.
        //
        // Historically, we had a bug that caused us to emit a query plan with 0 partitions, which
        // datafusion rejected.
        let table = crate::open_table("../test/tests/data/delta-2.2.0-partitioned-types")
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();

        let df = ctx
            .sql("select * from test where c3 = 100 ORDER BY c1 ASC")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();

        assert_eq!(actual.len(), 0);
    }

    #[tokio::test]
    async fn test_check_nullability() -> DeltaResult<()> {
        use arrow::array::StringArray;

        let data_checker = DeltaDataChecker {
            non_nullable_columns: vec!["zed".to_string(), "yap".to_string()],
            ..Default::default()
        };

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let nulls: Arc<dyn Array> = Arc::new(StringArray::new_null(1));
        let batch = RecordBatch::try_from_iter(vec![("a", arr), ("zed", nulls)]).unwrap();

        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr)]).unwrap();
        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr.clone()), ("yap", arr)]).unwrap();
        let _ = data_checker.check_nullability(&batch)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_uses_parquet_column_pruning() {
        let small: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["a"]));
        let large: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["b"
            .repeat(1024)
            .as_str()]));
        let batch = RecordBatch::try_from_iter(vec![("small", small), ("large", large)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap())
            .unwrap();

        let (object_store, mut operations) =
            RecordingObjectStore::new(table.log_store().object_store(None));
        // this uses an in memory store pointing at root...
        let both_store = Arc::new(object_store);
        let log_store = DefaultLogStore::new(
            both_store.clone(),
            both_store,
            table.log_store().config().clone(),
        );
        let provider = DeltaTableProvider::try_new(
            table.snapshot().unwrap().clone(),
            Arc::new(log_store),
            config,
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();
        let state = ctx.state();

        let df = ctx.sql("select small from test").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let mut stream = plan.execute(0, state.task_ctx()).unwrap();
        let Some(Ok(batch)) = stream.next().await else {
            panic!()
        };
        assert!(stream.next().await.is_none());
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());
        let small = batch.column_by_name("small").unwrap().as_string::<i32>();
        assert_eq!("a", small.iter().next().unwrap().unwrap());

        let expected = vec![
            ObjectStoreOperation::GetRange(LocationType::Data, 4952..4960),
            ObjectStoreOperation::GetRange(LocationType::Data, 2399..4952),
            #[expect(clippy::single_range_in_vec_init)]
            ObjectStoreOperation::GetRanges(LocationType::Data, vec![4..58]),
        ];
        let mut actual = Vec::new();
        operations.recv_many(&mut actual, 3).await;
        assert_eq!(expected, actual);
    }

    /// Records operations made by the inner object store on a channel obtained at construction
    struct RecordingObjectStore {
        inner: ObjectStoreRef,
        operations: UnboundedSender<ObjectStoreOperation>,
    }

    impl RecordingObjectStore {
        /// Returns an object store and a channel recording all operations made by the inner object store
        fn new(inner: ObjectStoreRef) -> (Self, UnboundedReceiver<ObjectStoreOperation>) {
            let (operations, operations_receiver) = unbounded_channel();
            (Self { inner, operations }, operations_receiver)
        }
    }

    impl Display for RecordingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            Display::fmt(&self.inner, f)
        }
    }

    impl Debug for RecordingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            Debug::fmt(&self.inner, f)
        }
    }

    #[derive(Debug, PartialEq)]
    enum ObjectStoreOperation {
        GetRanges(LocationType, Vec<Range<u64>>),
        GetRange(LocationType, Range<u64>),
        GetOpts(LocationType),
        Get(LocationType),
    }

    #[derive(Debug, PartialEq)]
    enum LocationType {
        Data,
        Commit,
    }

    impl From<&Path> for LocationType {
        fn from(value: &Path) -> Self {
            let dummy_url = Url::parse("dummy:///").unwrap();
            let parsed = ParsedLogPath::try_from(dummy_url.join(value.as_ref()).unwrap()).unwrap();
            if let Some(parsed) = parsed {
                if matches!(parsed.file_type, LogPathFileType::Commit) {
                    return LocationType::Commit;
                }
            }
            if value.to_string().starts_with("part-") {
                LocationType::Data
            } else {
                panic!("Unknown location type: {value:?}")
            }
        }
    }

    // Currently only read operations are recorded. Extend as necessary.
    #[async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put(
            &self,
            location: &Path,
            payload: PutPayload,
        ) -> object_store::Result<PutResult> {
            self.inner.put(location, payload).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart(
            &self,
            location: &Path,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart(location).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            self.operations
                .send(ObjectStoreOperation::Get(location.into()))
                .unwrap();
            self.inner.get(location).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.operations
                .send(ObjectStoreOperation::GetOpts(location.into()))
                .unwrap();
            self.inner.get_opts(location, options).await
        }

        async fn get_range(
            &self,
            location: &Path,
            range: Range<u64>,
        ) -> object_store::Result<Bytes> {
            self.operations
                .send(ObjectStoreOperation::GetRange(
                    location.into(),
                    range.clone(),
                ))
                .unwrap();
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.operations
                .send(ObjectStoreOperation::GetRanges(
                    location.into(),
                    ranges.to_vec(),
                ))
                .unwrap();
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn delete_stream<'a>(
            &'a self,
            locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }

        async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename_if_not_exists(from, to).await
        }
    }
}
