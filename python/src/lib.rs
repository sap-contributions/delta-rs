mod datafusion;
mod error;
mod features;
mod filesystem;
mod merge;
mod query;
mod reader;
mod schema;
mod utils;
mod writer;

use arrow::pyarrow::PyArrowType;
use arrow_schema::SchemaRef;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use datafusion_ffi::table_provider::FFI_TableProvider;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{MetadataValue, StructField};
use deltalake::arrow::{self, datatypes::Schema as ArrowSchema};
use deltalake::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::datasource::provider_as_source;
use deltalake::datafusion::logical_expr::LogicalPlanBuilder;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::MetadataExt;
use deltalake::kernel::{
    scalars::ScalarExt, Action, Add, LogicalFile, Remove, StructType, Transaction,
};
use deltalake::lakefs::LakeFSCustomExecuteHandler;
use deltalake::logstore::LogStoreRef;
use deltalake::logstore::{IORuntime, ObjectStoreRef};
use deltalake::operations::add_column::AddColumnBuilder;
use deltalake::operations::add_feature::AddTableFeatureBuilder;
use deltalake::operations::constraints::ConstraintBuilder;
use deltalake::operations::convert_to_delta::{ConvertToDeltaBuilder, PartitionStrategy};
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::drop_constraints::DropConstraintBuilder;
use deltalake::operations::filesystem_check::FileSystemCheckBuilder;
use deltalake::operations::load_cdf::CdfLoadBuilder;
use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::operations::restore::RestoreBuilder;
use deltalake::operations::set_tbl_properties::SetTablePropertiesBuilder;
use deltalake::operations::update::UpdateBuilder;
use deltalake::operations::update_field_metadata::UpdateFieldMetadataBuilder;
use deltalake::operations::update_table_metadata::{
    TableMetadataUpdate, UpdateTableMetadataBuilder,
};
use deltalake::operations::vacuum::{VacuumBuilder, VacuumMode};
use deltalake::operations::write::WriteBuilder;
use deltalake::operations::CustomExecuteHandler;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake::partitions::PartitionFilter;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::table::state::DeltaTableState;
use deltalake::{init_client_version, DeltaOps, DeltaResult, DeltaTableBuilder};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyDict, PyFrozenSet};
use pyo3::{prelude::*, IntoPyObjectExt};
use pyo3_arrow::export::{Arro3RecordBatch, Arro3RecordBatchReader};
use pyo3_arrow::{PyRecordBatchReader, PySchema as PyArrowSchema};
use schema::PySchema;
use serde_json::{Map, Value};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::future::IntoFuture;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use writer::maybe_lazy_cast_reader;

use crate::error::{DeltaError, DeltaProtocolError, PythonError};
use crate::features::TableFeatures;
use crate::filesystem::FsConfig;
use crate::merge::PyMergeBuilder;
use crate::query::PyQueryBuilder;
use crate::reader::convert_stream_to_reader;
use crate::schema::{schema_to_pyobject, Field};
use crate::utils::rt;
use crate::writer::to_lazy_table;

#[global_allocator]
#[cfg(all(target_family = "unix", not(target_os = "emscripten")))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[global_allocator]
#[cfg(any(not(target_family = "unix"), target_os = "emscripten"))]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(FromPyObject)]
enum PartitionFilterValue {
    Single(PyBackedStr),
    Multiple(Vec<PyBackedStr>),
}

#[pyclass(module = "deltalake._internal", frozen)]
struct RawDeltaTable {
    /// The internal reference to the table is guarded by a Mutex to allow for re-using the same
    /// [DeltaTable] instance across multiple Python threads
    _table: Arc<Mutex<deltalake::DeltaTable>>,
    // storing the config additionally on the table helps us make pickling work.
    _config: FsConfig,
}

#[pyclass(frozen)]
struct RawDeltaTableMetaData {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    name: Option<String>,
    #[pyo3(get)]
    description: Option<String>,
    #[pyo3(get)]
    partition_columns: Vec<String>,
    #[pyo3(get)]
    created_time: Option<i64>,
    #[pyo3(get)]
    configuration: HashMap<String, String>,
}

type StringVec = Vec<String>;

/// Segmented impl for RawDeltaTable to avoid these methods being exposed via the pymethods macro.
///
/// In essence all these functions should be considered internal to the Rust code and not exposed
/// up to the Python layer
impl RawDeltaTable {
    /// Internal helper method which allows for acquiring the lock on the underlying
    /// [deltalake::DeltaTable] and then executing the given function parameter with the guarded
    /// reference
    ///
    /// This should only be used for read-only accesses (duh) and callers that need to modify the
    /// underlying instance should acquire the lock themselves.
    ///
    fn with_table<T>(&self, func: impl Fn(&deltalake::DeltaTable) -> PyResult<T>) -> PyResult<T> {
        match self._table.lock() {
            Ok(table) => func(&table),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn object_store(&self) -> PyResult<ObjectStoreRef> {
        self.with_table(|t| Ok(t.log_store().object_store(None).clone()))
    }

    fn cloned_state(&self) -> PyResult<DeltaTableState> {
        self.with_table(|t| {
            t.snapshot()
                .cloned()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })
    }

    fn log_store(&self) -> PyResult<LogStoreRef> {
        self.with_table(|t| Ok(t.log_store().clone()))
    }

    fn set_state(&self, state: Option<DeltaTableState>) -> PyResult<()> {
        let mut original = self
            ._table
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        original.state = state;
        Ok(())
    }
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    #[pyo3(signature = (table_uri, version = None, storage_options = None, without_files = false, log_buffer_size = None))]
    fn new(
        py: Python,
        table_uri: &str,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let mut builder = deltalake::DeltaTableBuilder::from_valid_uri(table_uri)
                .map_err(error::PythonError::from)?
                .with_io_runtime(IORuntime::default());
            let options = storage_options.clone().unwrap_or_default();
            if let Some(storage_options) = storage_options {
                builder = builder.with_storage_options(storage_options)
            }
            if let Some(version) = version {
                builder = builder.with_version(version)
            }
            if without_files {
                builder = builder.without_files()
            }
            if let Some(buf_size) = log_buffer_size {
                builder = builder
                    .with_log_buffer_size(buf_size)
                    .map_err(PythonError::from)?;
            }

            let table = rt().block_on(builder.load()).map_err(PythonError::from)?;
            Ok(RawDeltaTable {
                _table: Arc::new(Mutex::new(table)),
                _config: FsConfig {
                    root_url: table_uri.into(),
                    options,
                },
            })
        })
    }

    #[pyo3(signature = (table_uri, storage_options = None))]
    #[staticmethod]
    pub fn is_deltatable(
        table_uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<bool> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        Ok(rt()
            .block_on(async {
                match builder.build() {
                    Ok(table) => table.verify_deltatable_existence().await,
                    Err(err) => Err(err),
                }
            })
            .map_err(PythonError::from)?)
    }

    pub fn table_uri(&self) -> PyResult<String> {
        self.with_table(|t| Ok(t.table_uri()))
    }

    pub fn version(&self) -> PyResult<Option<i64>> {
        self.with_table(|t| Ok(t.version()))
    }

    pub(crate) fn has_files(&self) -> PyResult<bool> {
        self.with_table(|t| Ok(t.config.require_files))
    }

    pub fn table_config(&self) -> PyResult<(bool, usize)> {
        self.with_table(|t| {
            let config = t.config.clone();
            // Require_files inverted to reflect without_files
            Ok((!config.require_files, config.log_buffer_size))
        })
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        let metadata = self.with_table(|t| {
            t.metadata()
                .cloned()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id().to_string(),
            name: metadata.name().map(String::from),
            description: metadata.description().map(String::from),
            partition_columns: metadata.partition_columns().clone(),
            created_time: metadata.created_time(),
            configuration: metadata.configuration().clone(),
        })
    }

    pub fn protocol_versions(&self) -> PyResult<(i32, i32, Option<StringVec>, Option<StringVec>)> {
        let table_protocol = self.with_table(|t| {
            t.protocol()
                .cloned()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        Ok((
            table_protocol.min_reader_version(),
            table_protocol.min_writer_version(),
            table_protocol.writer_features().and_then(|features| {
                let empty_set = !features.is_empty();
                empty_set.then(|| {
                    features
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                })
            }),
            table_protocol.reader_features().and_then(|features| {
                let empty_set = !features.is_empty();
                empty_set.then(|| {
                    features
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                })
            }),
        ))
    }

    /// Load the internal [RawDeltaTable] with the table state from the specified `version`
    ///
    /// This will acquire the internal lock since it is a mutating operation!
    pub fn load_version(&self, py: Python, version: i64) -> PyResult<()> {
        py.allow_threads(|| {
            #[allow(clippy::await_holding_lock)]
            rt().block_on(async {
                let mut table = self
                    ._table
                    .lock()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                (*table)
                    .load_version(version)
                    .await
                    .map_err(PythonError::from)
                    .map_err(PyErr::from)
            })
        })
    }

    /// Retrieve the latest version from the internally loaded table state
    pub fn get_latest_version(&self, py: Python) -> PyResult<i64> {
        py.allow_threads(|| {
            #[allow(clippy::await_holding_lock)]
            rt().block_on(async {
                match self._table.lock() {
                    Ok(table) => table
                        .get_latest_version()
                        .await
                        .map_err(PythonError::from)
                        .map_err(PyErr::from),
                    Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
                }
            })
        })
    }

    fn get_num_index_cols(&self) -> PyResult<i32> {
        self.with_table(|t| {
            Ok(t.snapshot()
                .map_err(PythonError::from)?
                .config()
                .num_indexed_cols())
        })
    }

    fn get_stats_columns(&self) -> PyResult<Option<Vec<String>>> {
        self.with_table(|t| {
            Ok(t.snapshot()
                .map_err(PythonError::from)?
                .config()
                .stats_columns()
                .map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<String>>()))
        })
    }

    pub fn load_with_datetime(&self, py: Python, ds: &str) -> PyResult<()> {
        py.allow_threads(|| {
            let datetime =
                DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds).map_err(
                    |err| PyValueError::new_err(format!("Failed to parse datetime string: {err}")),
                )?);
            #[allow(clippy::await_holding_lock)]
            rt().block_on(async {
                let mut table = self
                    ._table
                    .lock()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                (*table)
                    .load_with_datetime(datetime)
                    .await
                    .map_err(PythonError::from)
                    .map_err(PyErr::from)
            })
        })
    }

    #[pyo3(signature = (partition_filters=None))]
    pub fn files(
        &self,
        py: Python,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }
        py.allow_threads(|| {
            if let Some(filters) = partition_filters {
                let filters = convert_partition_filters(filters).map_err(PythonError::from)?;
                Ok(self
                    .with_table(|t| {
                        t.get_files_by_partitions(&filters)
                            .map_err(PythonError::from)
                            .map_err(PyErr::from)
                    })?
                    .into_iter()
                    .map(|p| p.to_string())
                    .collect())
            } else {
                match self._table.lock() {
                    Ok(table) => Ok(table
                        .get_files_iter()
                        .map_err(PythonError::from)?
                        .map(|f| f.to_string())
                        .collect()),
                    Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
                }
            }
        })
    }

    #[pyo3(signature = (partition_filters=None))]
    pub fn file_uris(
        &self,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if !self.with_table(|t| Ok(t.config.require_files))? {
            return Err(DeltaError::new_err("Table is initiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(PythonError::from)?;
            self.with_table(|t| {
                t.get_file_uris_by_partitions(&filters)
                    .map_err(PythonError::from)
                    .map_err(PyErr::from)
            })
        } else {
            self.with_table(|t| {
                Ok(t.get_file_uris()
                    .map_err(PythonError::from)
                    .map_err(PyErr::from)?
                    .collect::<Vec<String>>())
            })
        }
    }

    #[getter]
    pub fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let schema: StructType = self.with_table(|t| {
            t.get_schema()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
                .map(|s| s.to_owned())
        })?;
        schema_to_pyobject(schema, py)
    }

    /// Run the Vacuum command on the Delta Table: list and delete files no longer referenced
    /// by the Delta table and are older than the retention threshold.
    #[pyo3(signature = (dry_run, retention_hours = None, enforce_retention_duration = true,
    commit_properties=None, post_commithook_properties=None, full = false, keep_versions = None))]
    #[allow(clippy::too_many_arguments)]
    pub fn vacuum(
        &self,
        py: Python,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
        full: bool,
        keep_versions: Option<Vec<i64>>,
    ) -> PyResult<Vec<String>> {
        let (table, metrics) = py.allow_threads(|| {
            let snapshot = match self._table.lock() {
                Ok(table) => table
                    .snapshot()
                    .cloned()
                    .map_err(PythonError::from)
                    .map_err(PyErr::from),
                Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
            }?;
            let mut cmd = VacuumBuilder::new(self.log_store()?, snapshot)
                .with_enforce_retention_duration(enforce_retention_duration)
                .with_dry_run(dry_run);

            if full {
                cmd = cmd.with_mode(VacuumMode::Full);
            }

            if let Some(retention_period) = retention_hours {
                cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if let Some(keep_versions_vec) = keep_versions {
                cmd = cmd.with_keep_versions(keep_versions_vec.as_slice());
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(metrics.files_deleted)
    }

    /// Run the UPDATE command on the Delta Table
    #[pyo3(signature = (updates, predicate=None, writer_properties=None, safe_cast = false, commit_properties = None, post_commithook_properties=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &self,
        py: Python,
        updates: HashMap<String, String>,
        predicate: Option<String>,
        writer_properties: Option<PyWriterProperties>,
        safe_cast: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = UpdateBuilder::new(self.log_store()?, self.cloned_state()?)
                .with_safe_cast(safe_cast);

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            for (col_name, expression) in updates {
                cmd = cmd.with_update(col_name.clone(), expression.clone());
            }

            if let Some(update_predicate) = predicate {
                cmd = cmd.with_predicate(update_predicate);
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the optimize command on the Delta Table: merge small files into a large file by bin-packing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        partition_filters = None,
        target_size = None,
        max_concurrent_tasks = None,
        min_commit_interval = None,
        writer_properties=None,
        commit_properties=None,
        post_commithook_properties=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn compact_optimize(
        &self,
        py: Python,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        min_commit_interval: Option<u64>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = OptimizeBuilder::new(self.log_store()?, self.cloned_state()?)
                .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));
            if let Some(size) = target_size {
                cmd = cmd.with_target_size(size);
            }
            if let Some(commit_interval) = min_commit_interval {
                cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
            }

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            let converted_filters =
                convert_partition_filters(partition_filters.unwrap_or_default())
                    .map_err(PythonError::from)?;
            cmd = cmd.with_filters(&converted_filters);

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run z-order variation of optimize
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (z_order_columns,
        partition_filters = None,
        target_size = None,
        max_concurrent_tasks = None,
        max_spill_size = 20 * 1024 * 1024 * 1024,
        min_commit_interval = None,
        writer_properties=None,
        commit_properties=None,
        post_commithook_properties=None))]
    pub fn z_order_optimize(
        &self,
        py: Python,
        z_order_columns: Vec<String>,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        max_spill_size: usize,
        min_commit_interval: Option<u64>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = OptimizeBuilder::new(self.log_store()?, self.cloned_state()?)
                .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
                .with_max_spill_size(max_spill_size)
                .with_type(OptimizeType::ZOrder(z_order_columns));
            if let Some(size) = target_size {
                cmd = cmd.with_target_size(size);
            }
            if let Some(commit_interval) = min_commit_interval {
                cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
            }

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            let converted_filters =
                convert_partition_filters(partition_filters.unwrap_or_default())
                    .map_err(PythonError::from)?;
            cmd = cmd.with_filters(&converted_filters);

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[pyo3(signature = (fields, commit_properties=None, post_commithook_properties=None))]
    pub fn add_columns(
        &self,
        py: Python,
        fields: Vec<Field>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = AddColumnBuilder::new(self.log_store()?, self.cloned_state()?);

            let new_fields = fields
                .iter()
                .map(|v| v.inner.clone())
                .collect::<Vec<StructField>>();

            cmd = cmd.with_fields(new_fields);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (feature, allow_protocol_versions_increase, commit_properties=None, post_commithook_properties=None))]
    pub fn add_feature(
        &self,
        py: Python,
        feature: Vec<TableFeatures>,
        allow_protocol_versions_increase: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = AddTableFeatureBuilder::new(self.log_store()?, self.cloned_state()?)
                .with_features(feature)
                .with_allow_protocol_versions_increase(allow_protocol_versions_increase);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (constraints, commit_properties=None, post_commithook_properties=None))]
    pub fn add_constraints(
        &self,
        py: Python,
        constraints: HashMap<String, String>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = ConstraintBuilder::new(self.log_store()?, self.cloned_state()?);

            for (col_name, expression) in constraints {
                cmd = cmd.with_constraint(col_name.clone(), expression.clone());
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (name, raise_if_not_exists, commit_properties=None, post_commithook_properties=None))]
    pub fn drop_constraints(
        &self,
        py: Python,
        name: String,
        raise_if_not_exists: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = DropConstraintBuilder::new(self.log_store()?, self.cloned_state()?)
                .with_constraint(name)
                .with_raise_if_not_exists(raise_if_not_exists);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (starting_version = None, ending_version = None, starting_timestamp = None, ending_timestamp = None, columns = None, predicate = None, allow_out_of_range = false))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_cdf(
        &self,
        py: Python,
        starting_version: Option<i64>,
        ending_version: Option<i64>,
        starting_timestamp: Option<String>,
        ending_timestamp: Option<String>,
        columns: Option<Vec<String>>,
        predicate: Option<String>,
        allow_out_of_range: bool,
    ) -> PyResult<Arro3RecordBatchReader> {
        let ctx = SessionContext::new();
        let mut cdf_read = CdfLoadBuilder::new(self.log_store()?, self.cloned_state()?);

        if let Some(sv) = starting_version {
            cdf_read = cdf_read.with_starting_version(sv);
        }
        if let Some(ev) = ending_version {
            cdf_read = cdf_read.with_ending_version(ev);
        }
        if let Some(st) = starting_timestamp {
            let starting_ts: DateTime<Utc> = DateTime::<Utc>::from_str(&st)
                .map_err(|pe| PyValueError::new_err(pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_starting_timestamp(starting_ts);
        }
        if let Some(et) = ending_timestamp {
            let ending_ts = DateTime::<Utc>::from_str(&et)
                .map_err(|pe| PyValueError::new_err(pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_ending_timestamp(ending_ts);
        }

        if allow_out_of_range {
            cdf_read = cdf_read.with_allow_out_of_range();
        }

        let table_provider: Arc<dyn TableProvider> =
            Arc::new(DeltaCdfTableProvider::try_new(cdf_read).map_err(PythonError::from)?);

        let table_name: String = "source".to_string();

        ctx.register_table(table_name.clone(), table_provider)
            .map_err(PythonError::from)?;
        let select_string = if let Some(columns) = columns {
            columns.join(", ")
        } else {
            "*".to_string()
        };

        let sql = if let Some(predicate) = predicate {
            format!("SELECT {select_string} FROM `{table_name}` WHERE ({predicate}) ")
        } else {
            format!("SELECT {select_string} FROM `{table_name}`")
        };

        let stream = rt()
            .block_on(async { ctx.sql(&sql).await?.execute_stream().await })
            .map_err(PythonError::from)?;

        py.allow_threads(|| {
            let stream = convert_stream_to_reader(stream);
            Ok(stream.into())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        source,
        batch_schema,
        predicate,
        source_alias = None,
        target_alias = None,
        merge_schema = false,
        safe_cast = false,
        streamed_exec = false,
        writer_properties = None,
        post_commithook_properties = None,
        commit_properties = None,
    ))]
    pub fn create_merge_builder(
        &self,
        py: Python,
        source: PyRecordBatchReader,
        batch_schema: PyArrowSchema,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        merge_schema: bool,
        safe_cast: bool,
        streamed_exec: bool,
        writer_properties: Option<PyWriterProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<PyMergeBuilder> {
        py.allow_threads(|| {
            let handler: Option<Arc<dyn CustomExecuteHandler>> =
                if self.log_store()?.name() == "LakeFSLogStore" {
                    Some(Arc::new(LakeFSCustomExecuteHandler {}))
                } else {
                    None
                };

            Ok(PyMergeBuilder::new(
                self.log_store()?,
                self.cloned_state()?,
                source,
                batch_schema,
                predicate,
                source_alias,
                target_alias,
                merge_schema,
                safe_cast,
                streamed_exec,
                writer_properties,
                post_commithook_properties,
                commit_properties,
                handler,
            )
            .map_err(PythonError::from)?)
        })
    }

    #[pyo3(signature=(
        merge_builder
    ))]
    pub fn merge_execute(
        &self,
        py: Python,
        merge_builder: &mut PyMergeBuilder,
    ) -> PyResult<String> {
        py.allow_threads(|| {
            let (table, metrics) = merge_builder.execute().map_err(PythonError::from)?;
            self.set_state(table.state)?;
            Ok(metrics)
        })
    }

    // Run the restore command on the Delta Table: restore table to a given version or datetime
    #[pyo3(signature = (target, *, ignore_missing_files = false, protocol_downgrade_allowed = false, commit_properties=None))]
    pub fn restore(
        &self,
        target: Option<&Bound<'_, PyAny>>,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<String> {
        let mut cmd = RestoreBuilder::new(self.log_store()?, self.cloned_state()?);
        if let Some(val) = target {
            if let Ok(version) = val.extract::<i64>() {
                cmd = cmd.with_version_to_restore(version)
            }
            if let Ok(ds) = val.extract::<PyBackedStr>() {
                let datetime = DateTime::<Utc>::from(
                    DateTime::<FixedOffset>::parse_from_rfc3339(ds.as_ref()).map_err(|err| {
                        PyValueError::new_err(format!("Failed to parse datetime string: {err}"))
                    })?,
                );
                cmd = cmd.with_datetime_to_restore(datetime)
            }
        }
        cmd = cmd.with_ignore_missing_files(ignore_missing_files);
        cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if self.log_store()?.name() == "LakeFSLogStore" {
            cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the History command on the Delta Table: Returns provenance information, including the operation, user, and so on, for each write to a table.
    #[pyo3(signature = (limit=None))]
    pub fn history(&self, limit: Option<usize>) -> PyResult<Vec<String>> {
        #[allow(clippy::await_holding_lock)]
        let history = rt().block_on(async {
            match self._table.lock() {
                Ok(table) => table
                    .history(limit)
                    .await
                    .map_err(PythonError::from)
                    .map_err(PyErr::from),
                Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
            }
        })?;
        Ok(history
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect())
    }

    pub fn update_incremental(&self) -> PyResult<()> {
        #[allow(clippy::await_holding_lock)]
        #[allow(deprecated)]
        Ok(rt()
            .block_on(async {
                let mut table = self
                    ._table
                    .lock()
                    .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
                (*table).update_incremental(None).await
            })
            .map_err(PythonError::from)?)
    }

    #[pyo3(signature = (schema, partition_filters=None))]
    pub fn dataset_partitions<'py>(
        &self,
        py: Python<'py>,
        schema: PyArrowSchema,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<(String, Option<Bound<'py, PyAny>>)>> {
        let path_set = match partition_filters {
            Some(filters) => Some(HashSet::<_>::from_iter(
                self.files(py, Some(filters))?.iter().cloned(),
            )),
            None => None,
        };
        let stats_cols = self.get_stats_columns()?;
        let num_index_cols = self.get_num_index_cols()?;

        let schema = schema.into_inner();

        let inclusion_stats_cols = if let Some(stats_cols) = stats_cols {
            stats_cols
        } else if num_index_cols == -1 {
            schema
                .fields()
                .iter()
                .map(|v| v.name().clone())
                .collect::<Vec<_>>()
        } else if num_index_cols >= 0 {
            let mut fields = vec![];
            for idx in 0..(min(num_index_cols as usize, schema.fields.len())) {
                fields.push(schema.field(idx).name().clone())
            }
            fields
        } else {
            return Err(PythonError::from(DeltaTableError::generic(
                "Couldn't construct list of fields for file stats expression gatherings",
            )))?;
        };

        self.cloned_state()?
            .log_data()
            .into_iter()
            .filter_map(|f| {
                let path = f.path().to_string();
                match &path_set {
                    Some(path_set) => path_set.contains(&path).then_some((path, f)),
                    None => Some((path, f)),
                }
            })
            .map(|(path, f)| {
                let expression =
                    filestats_to_expression_next(py, &schema, &inclusion_stats_cols, f)?;
                Ok((path, expression))
            })
            .collect()
    }

    #[pyo3(signature = (partitions_filters=None))]
    fn get_active_partitions<'py>(
        &self,
        partitions_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyFrozenSet>> {
        let schema = self.with_table(|t| {
            t.get_schema()
                .cloned()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        let metadata = self.with_table(|t| {
            t.metadata()
                .cloned()
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        let column_names: HashSet<&str> =
            schema.fields().map(|field| field.name().as_str()).collect();
        let partition_columns: HashSet<&str> = metadata
            .partition_columns()
            .iter()
            .map(|col| col.as_str())
            .collect();

        if let Some(filters) = &partitions_filters {
            let unknown_columns: Vec<&PyBackedStr> = filters
                .iter()
                .map(|(column_name, _, _)| column_name)
                .filter(|column_name| {
                    let column_name: &'_ str = column_name.as_ref();
                    !column_names.contains(column_name)
                })
                .collect();
            if !unknown_columns.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "Filters include columns that are not in table schema: {unknown_columns:?}"
                )));
            }

            let non_partition_columns: Vec<&PyBackedStr> = filters
                .iter()
                .map(|(column_name, _, _)| column_name)
                .filter(|column_name| {
                    let column_name: &'_ str = column_name.as_ref();
                    !partition_columns.contains(column_name)
                })
                .collect();

            if !non_partition_columns.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "Filters include columns that are not partition columns: {non_partition_columns:?}"
                )));
            }
        }

        let converted_filters = convert_partition_filters(partitions_filters.unwrap_or_default())
            .map_err(PythonError::from)?;

        let partition_columns: Vec<&str> = partition_columns.into_iter().collect();

        let state = self.cloned_state()?;
        let adds = state
            .get_active_add_actions_by_partitions(&converted_filters)
            .map_err(PythonError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PythonError::from)?;
        let active_partitions: HashSet<Vec<(&str, Option<String>)>> = adds
            .iter()
            .flat_map(|add| {
                Ok::<_, PythonError>(
                    partition_columns
                        .iter()
                        .flat_map(|col| {
                            Ok::<_, PythonError>((
                                *col,
                                add.partition_values()
                                    .map_err(PythonError::from)?
                                    .get(*col)
                                    .map(|v| v.serialize()),
                            ))
                        })
                        .collect(),
                )
            })
            .collect();

        let active_partitions = active_partitions
            .into_iter()
            .map(|part| PyFrozenSet::new(py, part.iter()))
            .collect::<Result<Vec<Bound<'py, _>>, PyErr>>()?;
        PyFrozenSet::new(py, &active_partitions)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (add_actions, mode, partition_by, schema, partitions_filters=None, commit_properties=None, post_commithook_properties=None))]
    fn create_write_transaction(
        &self,
        py: Python,
        add_actions: Vec<PyAddAction>,
        mode: &str,
        partition_by: Vec<String>,
        schema: PyRef<PySchema>,
        partitions_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let schema = schema.as_ref().inner_type.clone();
        py.allow_threads(|| {
            let mode = mode.parse().map_err(PythonError::from)?;

            let existing_schema = self.with_table(|t| {
                t.get_schema()
                    .cloned()
                    .map_err(PythonError::from)
                    .map_err(PyErr::from)
            })?;

            let mut actions: Vec<Action> = add_actions
                .iter()
                .map(|add| Action::Add(add.into()))
                .collect();

            match mode {
                SaveMode::Overwrite => {
                    let converted_filters =
                        convert_partition_filters(partitions_filters.unwrap_or_default())
                            .map_err(PythonError::from)?;

                    let state = self.cloned_state()?;
                    let add_actions = state
                        .get_active_add_actions_by_partitions(&converted_filters)
                        .map_err(PythonError::from)?;

                    for old_add in add_actions {
                        let old_add = old_add.map_err(PythonError::from)?;
                        let remove_action = Action::Remove(Remove {
                            path: old_add.path().to_string(),
                            deletion_timestamp: Some(current_timestamp()),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(
                                old_add
                                    .partition_values()
                                    .map_err(PythonError::from)?
                                    .iter()
                                    .map(|(k, v)| {
                                        (
                                            k.to_string(),
                                            if v.is_null() {
                                                None
                                            } else {
                                                Some(v.serialize())
                                            },
                                        )
                                    })
                                    .collect(),
                            ),
                            size: Some(old_add.size()),
                            deletion_vector: None,
                            tags: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                        });
                        actions.push(remove_action);
                    }

                    // Update metadata with new schema
                    if schema != existing_schema {
                        let mut metadata = self.with_table(|t| {
                            t.metadata()
                                .cloned()
                                .map_err(PythonError::from)
                                .map_err(PyErr::from)
                        })?;
                        metadata = metadata
                            .with_schema(&schema)
                            .map_err(DeltaTableError::from)
                            .map_err(PythonError::from)?;
                        actions.push(Action::Metadata(metadata));
                    }
                }
                _ => {
                    // This should be unreachable from Python
                    if schema != existing_schema {
                        DeltaProtocolError::new_err("Cannot change schema except in overwrite.");
                    }
                }
            }

            let operation = DeltaOperation::Write {
                mode,
                partition_by: Some(partition_by),
                predicate: None,
            };

            let mut properties = CommitProperties::default();
            if let Some(props) = commit_properties {
                if let Some(metadata) = props.custom_metadata {
                    let json_metadata: Map<String, Value> =
                        metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
                    properties = properties.with_metadata(json_metadata);
                };

                if let Some(max_retries) = props.max_commit_retries {
                    properties = properties.with_max_retries(max_retries);
                };
            }

            if let Some(post_commit_hook_props) = post_commithook_properties {
                properties = set_post_commithook_properties(properties, post_commit_hook_props)
            }

            rt().block_on(
                CommitBuilder::from(properties)
                    .with_actions(actions)
                    .build(Some(&self.cloned_state()?), self.log_store()?, operation)
                    .into_future(),
            )
            .map_err(PythonError::from)?;

            Ok(())
        })
    }

    pub fn create_checkpoint(&self, py: Python) -> PyResult<()> {
        py.allow_threads(|| {
            let operation_id = Uuid::new_v4();
            let handle = Arc::new(LakeFSCustomExecuteHandler {});
            let store = &self.log_store()?;

            // Runs lakefs pre-execution
            if store.name() == "LakeFSLogStore" {
                #[allow(clippy::await_holding_lock)]
                rt().block_on(async {
                    handle
                        .before_post_commit_hook(store, true, operation_id)
                        .await
                })
                .map_err(PythonError::from)?;
            }

            #[allow(clippy::await_holding_lock)]
            let result = rt().block_on(async {
                match self._table.lock() {
                    Ok(table) => create_checkpoint(&table, Some(operation_id))
                        .await
                        .map_err(PythonError::from)
                        .map_err(PyErr::from),
                    Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
                }
            });

            // Runs lakefs post-execution for file operations
            if store.name() == "LakeFSLogStore" {
                rt().block_on(async {
                    handle
                        .after_post_commit_hook(store, true, operation_id)
                        .await
                })
                .map_err(PythonError::from)?;
            }
            result
        })?;

        Ok(())
    }

    pub fn cleanup_metadata(&self, py: Python) -> PyResult<()> {
        let (_result, new_state) = py.allow_threads(|| {
            let operation_id = Uuid::new_v4();
            let handle = Arc::new(LakeFSCustomExecuteHandler {});
            let store = &self.log_store()?;

            // Runs lakefs pre-execution
            if store.name() == "LakeFSLogStore" {
                #[allow(clippy::await_holding_lock)]
                rt().block_on(async {
                    handle
                        .before_post_commit_hook(store, true, operation_id)
                        .await
                })
                .map_err(PythonError::from)?;
            }

            #[allow(clippy::await_holding_lock)]
            let result = rt().block_on(async {
                match self._table.lock() {
                    Ok(table) => {
                        let result = cleanup_metadata(&table, Some(operation_id))
                            .await
                            .map_err(PythonError::from)
                            .map_err(PyErr::from)?;

                        let new_state = if result > 0 {
                            Some(
                                DeltaTableState::try_new(
                                    &table.log_store(),
                                    table.config.clone(),
                                    table.version(),
                                )
                                .await
                                .map_err(PythonError::from)?,
                            )
                        } else {
                            None
                        };

                        Ok((result, new_state))
                    }
                    Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
                }
            });

            // Runs lakefs post-execution for file operations
            if store.name() == "LakeFSLogStore" {
                rt().block_on(async {
                    handle
                        .after_post_commit_hook(store, true, operation_id)
                        .await
                })
                .map_err(PythonError::from)?;
            }
            result
        })?;

        if new_state.is_some() {
            self.set_state(new_state)?;
        }

        Ok(())
    }

    pub fn get_add_actions(&self, flatten: bool) -> PyResult<Arro3RecordBatch> {
        // replace with Arro3RecordBatch once new release is done for arro3.core
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }
        let batch = self.with_table(|t| {
            Ok(t.snapshot()
                .map_err(PythonError::from)?
                .add_actions_table(flatten)
                .map_err(PythonError::from)?)
        })?;
        Ok(batch.into())
    }

    pub fn get_add_file_sizes(&self) -> PyResult<HashMap<String, i64>> {
        self.with_table(|t| {
            Ok(t.snapshot()
                .map_err(PythonError::from)?
                .snapshot()
                .files()
                .map(|f| (f.path().to_string(), f.size()))
                .collect::<HashMap<String, i64>>())
        })
    }

    /// Run the delete command on the delta table: delete records following a predicate and return the delete metrics.
    #[pyo3(signature = (predicate = None, writer_properties=None, commit_properties=None, post_commithook_properties=None))]
    pub fn delete(
        &self,
        py: Python,
        predicate: Option<String>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = DeleteBuilder::new(self.log_store()?, self.cloned_state()?);
            if let Some(predicate) = predicate {
                cmd = cmd.with_predicate(predicate);
            }
            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }
            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[pyo3(signature = (properties, raise_if_not_exists, commit_properties=None))]
    pub fn set_table_properties(
        &self,
        properties: HashMap<String, String>,
        raise_if_not_exists: bool,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<()> {
        let mut cmd = SetTablePropertiesBuilder::new(self.log_store()?, self.cloned_state()?)
            .with_properties(properties)
            .with_raise_if_not_exists(raise_if_not_exists);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if self.log_store()?.name() == "LakeFSLogStore" {
            cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        let table = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (name, commit_properties=None))]
    pub fn set_table_name(
        &self,
        name: String,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<()> {
        let update = TableMetadataUpdate {
            name: Some(name),
            description: None,
        };
        let mut cmd = UpdateTableMetadataBuilder::new(self.log_store()?, self.cloned_state()?)
            .with_update(update);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if self.log_store()?.name() == "LakeFSLogStore" {
            cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}));
        }

        let table = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[pyo3(signature = (description, commit_properties=None))]
    pub fn set_table_description(
        &self,
        description: String,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<()> {
        let update = TableMetadataUpdate {
            name: None,
            description: Some(description),
        };
        let mut cmd = UpdateTableMetadataBuilder::new(self.log_store()?, self.cloned_state()?)
            .with_update(update);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if self.log_store()?.name() == "LakeFSLogStore" {
            cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}));
        }

        let table = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    /// Execute the File System Check command (FSCK) on the delta table: removes old reference to files that
    /// have been deleted or are malformed
    #[pyo3(signature = (dry_run = true, commit_properties = None, post_commithook_properties=None))]
    pub fn repair(
        &self,
        dry_run: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let mut cmd = FileSystemCheckBuilder::new(self.log_store()?, self.cloned_state()?)
            .with_dry_run(dry_run);

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if self.log_store()?.name() == "LakeFSLogStore" {
            cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Get the latest transaction version for the given application ID.
    ///
    /// Returns `None` if the application ID is not found.
    pub fn transaction_version(&self, app_id: String) -> PyResult<Option<i64>> {
        // NOTE: this will simplify once we have moved logstore onto state.
        let log_store = self.log_store()?;
        let snapshot = self.with_table(|t| Ok(t.snapshot().map_err(PythonError::from)?.clone()))?;
        Ok(rt()
            .block_on(snapshot.transaction_version(log_store.as_ref(), app_id))
            .map_err(PythonError::from)?)
    }

    #[pyo3(signature = (field_name, metadata, commit_properties=None, post_commithook_properties=None))]
    pub fn set_column_metadata(
        &self,
        py: Python,
        field_name: &str,
        metadata: HashMap<String, String>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = UpdateFieldMetadataBuilder::new(self.log_store()?, self.cloned_state()?);

            cmd = cmd.with_field_name(field_name).with_metadata(
                metadata
                    .iter()
                    .map(|(k, v)| (k.clone(), MetadataValue::String(v.clone())))
                    .collect(),
            );

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties)
            }

            if self.log_store()?.name() == "LakeFSLogStore" {
                cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(cmd.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;
        self.set_state(table.state)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (data, batch_schema, mode, schema_mode=None, partition_by=None, predicate=None, target_file_size=None, name=None, description=None, configuration=None, writer_properties=None, commit_properties=None, post_commithook_properties=None))]
    fn write(
        &self,
        py: Python,
        data: PyRecordBatchReader,
        batch_schema: PyArrowSchema,
        mode: String,
        schema_mode: Option<String>,
        partition_by: Option<Vec<String>>,
        predicate: Option<String>,
        target_file_size: Option<usize>,
        name: Option<String>,
        description: Option<String>,
        configuration: Option<HashMap<String, Option<String>>>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let save_mode = mode.parse().map_err(PythonError::from)?;

            let mut builder = WriteBuilder::new(
                self.log_store()?,
                self.with_table(|t| Ok(t.state.clone()))?,
                // Take the Option<state> since it might be the first write,
                // triggered through `write_to_deltalake`
            )
            .with_save_mode(save_mode);

            let table_provider = to_lazy_table(maybe_lazy_cast_reader(
                data.into_reader()?,
                batch_schema.into_inner(),
            ))
            .map_err(PythonError::from)?;

            let plan = LogicalPlanBuilder::scan("source", provider_as_source(table_provider), None)
                .map_err(PythonError::from)?
                .build()
                .map_err(PythonError::from)?;

            builder = builder.with_input_execution_plan(Arc::new(plan));

            if let Some(schema_mode) = schema_mode {
                builder = builder.with_schema_mode(schema_mode.parse().map_err(PythonError::from)?);
            }
            if let Some(partition_columns) = partition_by {
                builder = builder.with_partition_columns(partition_columns);
            }

            if let Some(writer_props) = writer_properties {
                builder = builder.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            if let Some(name) = &name {
                builder = builder.with_table_name(name);
            };

            if let Some(description) = &description {
                builder = builder.with_description(description);
            };

            if let Some(predicate) = predicate {
                builder = builder.with_replace_where(predicate);
            };

            if let Some(target_file_size) = target_file_size {
                builder = builder.with_target_file_size(target_file_size)
            };

            if let Some(config) = configuration {
                builder = builder.with_configuration(config);
            };

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                builder = builder.with_commit_properties(commit_properties);
            };

            if self.log_store()?.name() == "LakeFSLogStore" {
                builder =
                    builder.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
            }

            rt().block_on(builder.into_future())
                .map_err(PythonError::from)
                .map_err(PyErr::from)
        })?;

        self.set_state(table.state)?;
        Ok(())
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // tokio runtime handle?
        let handle = None;
        let name = CString::new("datafusion_table_provider").unwrap();

        let table = self.with_table(|t| Ok(Arc::new(t.clone())))?;
        let provider = FFI_TableProvider::new(table, false, handle);

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}

fn set_post_commithook_properties(
    mut commit_properties: CommitProperties,
    post_commithook_properties: PyPostCommitHookProperties,
) -> CommitProperties {
    commit_properties =
        commit_properties.with_create_checkpoint(post_commithook_properties.create_checkpoint);
    commit_properties = commit_properties
        .with_cleanup_expired_logs(post_commithook_properties.cleanup_expired_logs);
    commit_properties
}

fn set_writer_properties(writer_properties: PyWriterProperties) -> DeltaResult<WriterProperties> {
    let mut properties = WriterProperties::builder();
    let data_page_size_limit = writer_properties.data_page_size_limit;
    let dictionary_page_size_limit = writer_properties.dictionary_page_size_limit;
    let data_page_row_count_limit = writer_properties.data_page_row_count_limit;
    let write_batch_size = writer_properties.write_batch_size;
    let max_row_group_size = writer_properties.max_row_group_size;
    let compression = writer_properties.compression;
    let statistics_truncate_length = writer_properties.statistics_truncate_length;
    let default_column_properties = writer_properties.default_column_properties;
    let column_properties = writer_properties.column_properties;

    if let Some(data_page_size) = data_page_size_limit {
        properties = properties.set_data_page_size_limit(data_page_size);
    }
    if let Some(dictionary_page_size) = dictionary_page_size_limit {
        properties = properties.set_dictionary_page_size_limit(dictionary_page_size);
    }
    if let Some(data_page_row_count) = data_page_row_count_limit {
        properties = properties.set_data_page_row_count_limit(data_page_row_count);
    }
    if let Some(batch_size) = write_batch_size {
        properties = properties.set_write_batch_size(batch_size);
    }
    if let Some(row_group_size) = max_row_group_size {
        properties = properties.set_max_row_group_size(row_group_size);
    }
    properties = properties.set_statistics_truncate_length(statistics_truncate_length);

    if let Some(compression) = compression {
        let compress: Compression = compression
            .parse()
            .map_err(|err: ParquetError| DeltaTableError::Generic(err.to_string()))?;

        properties = properties.set_compression(compress);
    }

    if let Some(default_column_properties) = default_column_properties {
        if let Some(dictionary_enabled) = default_column_properties.dictionary_enabled {
            properties = properties.set_dictionary_enabled(dictionary_enabled);
        }
        if let Some(statistics_enabled) = default_column_properties.statistics_enabled {
            let enabled_statistics: EnabledStatistics = statistics_enabled
                .parse()
                .map_err(|err: String| DeltaTableError::Generic(err))?;

            properties = properties.set_statistics_enabled(enabled_statistics);
        }
        if let Some(bloom_filter_properties) = default_column_properties.bloom_filter_properties {
            if let Some(set_bloom_filter_enabled) = bloom_filter_properties.set_bloom_filter_enabled
            {
                properties = properties.set_bloom_filter_enabled(set_bloom_filter_enabled);
            }
            if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                properties = properties.set_bloom_filter_fpp(bloom_filter_fpp);
            }
            if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                properties = properties.set_bloom_filter_ndv(bloom_filter_ndv);
            }
        }
    }
    if let Some(column_properties) = column_properties {
        for (column_name, column_prop) in column_properties {
            if let Some(column_prop) = column_prop {
                if let Some(dictionary_enabled) = column_prop.dictionary_enabled {
                    properties = properties.set_column_dictionary_enabled(
                        column_name.clone().into(),
                        dictionary_enabled,
                    );
                }
                if let Some(statistics_enabled) = column_prop.statistics_enabled {
                    let enabled_statistics: EnabledStatistics = statistics_enabled
                        .parse()
                        .map_err(|err: String| DeltaTableError::Generic(err))?;

                    properties = properties.set_column_statistics_enabled(
                        column_name.clone().into(),
                        enabled_statistics,
                    );
                }
                if let Some(bloom_filter_properties) = column_prop.bloom_filter_properties {
                    if let Some(set_bloom_filter_enabled) =
                        bloom_filter_properties.set_bloom_filter_enabled
                    {
                        properties = properties.set_column_bloom_filter_enabled(
                            column_name.clone().into(),
                            set_bloom_filter_enabled,
                        );
                    }
                    if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                        properties = properties.set_column_bloom_filter_fpp(
                            column_name.clone().into(),
                            bloom_filter_fpp,
                        );
                    }
                    if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                        properties = properties
                            .set_column_bloom_filter_ndv(column_name.into(), bloom_filter_ndv);
                    }
                }
            }
        }
    }
    Ok(properties.build())
}

fn convert_partition_filters(
    partitions_filters: Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: &'_ str = v.as_ref();
                PartitionFilter::try_from((key, op, v))
            }
            (key, op, PartitionFilterValue::Multiple(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: Vec<&'_ str> = v.iter().map(|v| v.as_ref()).collect();
                PartitionFilter::try_from((key, op, v.as_slice()))
            }
        })
        .collect()
}

fn maybe_create_commit_properties(
    maybe_commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> Option<CommitProperties> {
    if maybe_commit_properties.is_none() && post_commithook_properties.is_none() {
        return None;
    }
    let mut commit_properties = CommitProperties::default();

    if let Some(commit_props) = maybe_commit_properties {
        if let Some(metadata) = commit_props.custom_metadata {
            let json_metadata: Map<String, Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            commit_properties = commit_properties.with_metadata(json_metadata);
        };

        if let Some(max_retries) = commit_props.max_commit_retries {
            commit_properties = commit_properties.with_max_retries(max_retries);
        };

        if let Some(app_transactions) = commit_props.app_transactions {
            let app_transactions = app_transactions.iter().map(Transaction::from).collect();
            commit_properties = commit_properties.with_application_transactions(app_transactions);
        }
    }

    if let Some(post_commit_hook_props) = post_commithook_properties {
        commit_properties =
            set_post_commithook_properties(commit_properties, post_commit_hook_props)
    }
    Some(commit_properties)
}

fn scalar_to_py<'py>(value: &Scalar, py_date: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    use Scalar::*;

    let py = py_date.py();
    let val = match value {
        Null(_) => py.None(),
        Boolean(val) => val.into_py_any(py)?,
        Binary(val) => val.into_py_any(py)?,
        String(val) => val.into_py_any(py)?,
        Byte(val) => val.into_py_any(py)?,
        Short(val) => val.into_py_any(py)?,
        Integer(val) => val.into_py_any(py)?,
        Long(val) => val.into_py_any(py)?,
        Float(val) => val.into_py_any(py)?,
        Double(val) => val.into_py_any(py)?,
        Timestamp(_) => {
            // We need to manually append 'Z' add to end so that pyarrow can cast the
            // scalar value to pa.timestamp("us","UTC")
            let value = value.serialize();
            format!("{value}Z").into_py_any(py)?
        }
        TimestampNtz(_) => {
            let value = value.serialize();
            value.into_py_any(py)?
        }
        // NOTE: PyArrow 13.0.0 lost the ability to cast from string to date32, so
        // we have to implement that manually.
        Date(_) => {
            let date = py_date.call_method1("fromisoformat", (value.serialize(),))?;
            date.into_py_any(py)?
        }
        Decimal(_) => value.serialize().into_py_any(py)?,
        Struct(data) => {
            let py_struct = PyDict::new(py);
            for (field, value) in data.fields().iter().zip(data.values().iter()) {
                py_struct.set_item(field.name(), scalar_to_py(value, py_date)?)?;
            }
            py_struct.into_py_any(py)?
        }
        Array(_val) => todo!("how should this be converted!"),
        Map(map) => {
            let py_map = PyDict::new(py);
            for (key, value) in map.pairs() {
                py_map.set_item(scalar_to_py(key, py_date)?, scalar_to_py(value, py_date)?)?;
            }
            py_map.into_py_any(py)?
        }
    };

    Ok(val.into_bound(py))
}

/// Create expression that file statistics guarantee to be true.
///
/// PyArrow uses this expression to determine which Dataset fragments may be
/// skipped during a scan.
///
/// Partition values are translated to equality expressions (if they are valid)
/// or is_null expression otherwise. For example, if the partition is
/// {"date": "2021-01-01", "x": null}, then the expression is:
/// field(date) = "2021-01-01" AND x IS NULL
///
/// Statistics are translated into inequalities. If there are null values, then
/// they must be OR'd with is_null.
fn filestats_to_expression_next<'py>(
    py: Python<'py>,
    schema: &SchemaRef,
    stats_columns: &[String],
    file_info: LogicalFile<'_>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let ds = PyModule::import(py, "pyarrow.dataset")?;
    let py_field = ds.getattr("field")?;
    let pa = PyModule::import(py, "pyarrow")?;
    let py_date = Python::import(py, "datetime")?.getattr("date")?;
    let mut expressions = Vec::new();

    let cast_to_type = |column_name: &String, value: &Bound<'py, PyAny>, schema: &ArrowSchema| {
        let column_type = schema
            .field_with_name(column_name)
            .map_err(|_| {
                PyValueError::new_err(format!("Column not found in schema: {column_name}"))
            })?
            .data_type()
            .clone();
        let column_type = PyArrowType(column_type).into_pyobject(py)?;
        pa.call_method1("scalar", (value,))?
            .call_method1("cast", (column_type,))
    };

    if let Ok(partitions_values) = file_info.partition_values() {
        for (column, value) in partitions_values.iter() {
            let column = column.to_string();
            if !value.is_null() {
                // value is a string, but needs to be parsed into appropriate type
                let converted_value =
                    cast_to_type(&column, &scalar_to_py(value, &py_date)?, schema)?;
                expressions.push(
                    py_field
                        .call1((&column,))?
                        .call_method1("__eq__", (converted_value,)),
                );
            } else {
                expressions.push(py_field.call1((column,))?.call_method0("is_null"));
            }
        }
    }

    let mut has_nulls_set: HashSet<String> = HashSet::new();

    // NOTE: null_counts should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.null_counts() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            if stats_columns.contains(field.name()) {
                if let Scalar::Long(val) = value {
                    if *val == 0 {
                        expressions.push(py_field.call1((field.name(),))?.call_method0("is_valid"));
                    } else if Some(*val as usize) == file_info.num_records() {
                        expressions.push(py_field.call1((field.name(),))?.call_method0("is_null"));
                    } else {
                        has_nulls_set.insert(field.name().to_string());
                    }
                }
            }
        }
    }

    // NOTE: min_values should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.min_values() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            if stats_columns.contains(field.name()) {
                match value {
                    // TODO: Handle nested field statistics.
                    Scalar::Struct(_) => {}
                    _ => {
                        let maybe_minimum =
                            cast_to_type(field.name(), &scalar_to_py(value, &py_date)?, schema);
                        if let Ok(minimum) = maybe_minimum {
                            let field_expr = py_field.call1((field.name(),))?;
                            let expr = field_expr.call_method1("__ge__", (minimum,));
                            let expr = if has_nulls_set.contains(field.name()) {
                                // col >= min_value OR col is null
                                let is_null_expr = field_expr.call_method0("is_null");
                                expr?.call_method1("__or__", (is_null_expr?,))
                            } else {
                                // col >= min_value
                                expr
                            };
                            expressions.push(expr);
                        }
                    }
                }
            }
        }
    }

    // NOTE: max_values should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.max_values() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            if stats_columns.contains(field.name()) {
                match value {
                    // TODO: Handle nested field statistics.
                    Scalar::Struct(_) => {}
                    _ => {
                        let maybe_maximum =
                            cast_to_type(field.name(), &scalar_to_py(value, &py_date)?, schema);
                        if let Ok(maximum) = maybe_maximum {
                            let field_expr = py_field.call1((field.name(),))?;
                            let expr = field_expr.call_method1("__le__", (maximum,));
                            let expr = if has_nulls_set.contains(field.name()) {
                                // col <= max_value OR col is null
                                let is_null_expr = field_expr.call_method0("is_null");
                                expr?.call_method1("__or__", (is_null_expr?,))
                            } else {
                                // col <= max_value
                                expr
                            };
                            expressions.push(expr);
                        }
                    }
                }
            }
        }
    }

    if expressions.is_empty() {
        Ok(None)
    } else {
        expressions
            .into_iter()
            .reduce(|accum, item| accum?.call_method1("__and__", (item?,)))
            .transpose()
    }
}

#[pyfunction]
fn rust_core_version() -> &'static str {
    deltalake::crate_version()
}

fn current_timestamp() -> i64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis().try_into().unwrap()
}

#[derive(FromPyObject)]
pub struct PyAddAction {
    path: String,
    size: i64,
    partition_values: HashMap<String, Option<String>>,
    modification_time: i64,
    data_change: bool,
    stats: Option<String>,
}

impl From<&PyAddAction> for Add {
    fn from(action: &PyAddAction) -> Self {
        Add {
            path: action.path.clone(),
            size: action.size,
            partition_values: action.partition_values.clone(),
            modification_time: action.modification_time,
            data_change: action.data_change,
            stats: action.stats.clone(),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }
    }
}

#[derive(FromPyObject)]
pub struct BloomFilterProperties {
    pub set_bloom_filter_enabled: Option<bool>,
    pub fpp: Option<f64>,
    pub ndv: Option<u64>,
}

#[derive(FromPyObject)]
pub struct ColumnProperties {
    pub dictionary_enabled: Option<bool>,
    pub statistics_enabled: Option<String>,
    pub bloom_filter_properties: Option<BloomFilterProperties>,
}

#[derive(FromPyObject)]
pub struct PyWriterProperties {
    data_page_size_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    write_batch_size: Option<usize>,
    max_row_group_size: Option<usize>,
    statistics_truncate_length: Option<usize>,
    compression: Option<String>,
    default_column_properties: Option<ColumnProperties>,
    column_properties: Option<HashMap<String, Option<ColumnProperties>>>,
}

#[derive(FromPyObject)]
pub struct PyPostCommitHookProperties {
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
}

#[derive(Clone)]
#[pyclass(name = "Transaction", module = "deltalake._internal")]
pub struct PyTransaction {
    #[pyo3(get)]
    pub app_id: String,
    #[pyo3(get)]
    pub version: i64,
    #[pyo3(get)]
    pub last_updated: Option<i64>,
}

#[pymethods]
impl PyTransaction {
    #[new]
    #[pyo3(signature = (app_id, version, last_updated = None))]
    fn new(app_id: String, version: i64, last_updated: Option<i64>) -> Self {
        Self {
            app_id,
            version,
            last_updated,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Transaction(app_id={}, version={}, last_updated={})",
            self.app_id,
            self.version,
            self.last_updated
                .map_or("None".to_owned(), |n| n.to_string())
        )
    }
}

impl From<Transaction> for PyTransaction {
    fn from(value: Transaction) -> Self {
        PyTransaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<&PyTransaction> for Transaction {
    fn from(value: &PyTransaction) -> Self {
        Transaction {
            app_id: value.app_id.clone(),
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

#[derive(FromPyObject)]
pub struct PyCommitProperties {
    custom_metadata: Option<HashMap<String, String>>,
    max_commit_retries: Option<usize>,
    app_transactions: Option<Vec<PyTransaction>>,
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (table_uri, data, batch_schema, mode, schema_mode=None, partition_by=None, predicate=None, target_file_size=None, name=None, description=None, configuration=None, storage_options=None, writer_properties=None, commit_properties=None, post_commithook_properties=None))]
fn write_to_deltalake(
    py: Python,
    table_uri: String,
    data: PyRecordBatchReader,
    batch_schema: PyArrowSchema,
    mode: String,
    schema_mode: Option<String>,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    target_file_size: Option<usize>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    writer_properties: Option<PyWriterProperties>,
    commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> PyResult<()> {
    let raw_table: DeltaResult<RawDeltaTable> = py.allow_threads(|| {
        let options = storage_options.clone().unwrap_or_default();
        let table = rt()
            .block_on(DeltaOps::try_from_uri_with_storage_options(
                &table_uri,
                options.clone(),
            ))?
            .0;

        let raw_table = RawDeltaTable {
            _table: Arc::new(Mutex::new(table)),
            _config: FsConfig {
                root_url: table_uri,
                options,
            },
        };
        Ok(raw_table)
    });

    raw_table.map_err(PythonError::from)?.write(
        py,
        data,
        batch_schema,
        mode,
        schema_mode,
        partition_by,
        predicate,
        target_file_size,
        name,
        description,
        configuration,
        writer_properties,
        commit_properties,
        post_commithook_properties,
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (table_uri, schema, partition_by, mode, raise_if_key_not_exists, name=None, description=None, configuration=None, storage_options=None, commit_properties=None, post_commithook_properties=None))]
fn create_deltalake(
    py: Python,
    table_uri: String,
    schema: PyRef<PySchema>,
    partition_by: Vec<String>,
    mode: String,
    raise_if_key_not_exists: bool,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> PyResult<()> {
    let schema = schema.as_ref().inner_type.clone();
    py.allow_threads(|| {
        let table = DeltaTableBuilder::from_uri(table_uri.clone())
            .with_storage_options(storage_options.unwrap_or_default())
            .build()
            .map_err(PythonError::from)?;

        let mode = mode.parse().map_err(PythonError::from)?;

        let use_lakefs_handler = table.log_store().name() == "LakeFSLogStore";

        let mut builder = DeltaOps(table)
            .create()
            .with_columns(schema.fields().cloned())
            .with_save_mode(mode)
            .with_raise_if_key_not_exists(raise_if_key_not_exists)
            .with_partition_columns(partition_by);

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        };

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        };

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            builder = builder.with_commit_properties(commit_properties);
        };

        if use_lakefs_handler {
            builder = builder.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;

        Ok(())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (table_uri, schema, add_actions, mode, partition_by, name=None, description=None, configuration=None, storage_options=None, commit_properties=None, post_commithook_properties=None))]
fn create_table_with_add_actions(
    py: Python,
    table_uri: String,
    schema: PyRef<PySchema>,
    add_actions: Vec<PyAddAction>,
    mode: &str,
    partition_by: Vec<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> PyResult<()> {
    let schema = schema.as_ref().inner_type.clone();

    py.allow_threads(|| {
        let table = DeltaTableBuilder::from_uri(table_uri.clone())
            .with_storage_options(storage_options.unwrap_or_default())
            .build()
            .map_err(PythonError::from)?;

        let use_lakefs_handler = table.log_store().name() == "LakeFSLogStore";

        let mut builder = DeltaOps(table)
            .create()
            .with_columns(schema.fields().cloned())
            .with_partition_columns(partition_by)
            .with_actions(add_actions.iter().map(|add| Action::Add(add.into())))
            .with_save_mode(SaveMode::from_str(mode).map_err(PythonError::from)?);

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        };

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        };

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            builder = builder.with_commit_properties(commit_properties);
        };

        if use_lakefs_handler {
            builder = builder.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;

        Ok(())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (uri, partition_schema=None, partition_strategy=None, name=None, description=None, configuration=None, storage_options=None, commit_properties=None, post_commithook_properties=None))]
fn convert_to_deltalake(
    py: Python,
    uri: String,
    partition_schema: Option<PyRef<PySchema>>,
    partition_strategy: Option<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> PyResult<()> {
    let partition_schema = partition_schema.map(|s| s.as_ref().inner_type.clone());
    py.allow_threads(|| {
        let mut builder = ConvertToDeltaBuilder::new().with_location(uri.clone());

        if let Some(part_schema) = partition_schema {
            builder = builder.with_partition_schema(part_schema.fields().cloned());
        }

        if let Some(partition_strategy) = &partition_strategy {
            let strategy: PartitionStrategy =
                partition_strategy.parse().map_err(PythonError::from)?;
            builder = builder.with_partition_strategy(strategy);
        }

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        }

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        }

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(strg_options) = storage_options {
            builder = builder.with_storage_options(strg_options);
        };

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            builder = builder.with_commit_properties(commit_properties);
        };

        if uri.starts_with("lakefs://") {
            builder = builder.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
        }

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (table=None, configuration=None))]
fn get_num_idx_cols_and_stats_columns(
    table: Option<&RawDeltaTable>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> PyResult<(i32, Option<Vec<String>>)> {
    match table.as_ref() {
        Some(table) => Ok(deltalake::operations::get_num_idx_cols_and_stats_columns(
            Some(table.cloned_state()?.table_config()),
            configuration.unwrap_or_default(),
        )),
        None => Ok(deltalake::operations::get_num_idx_cols_and_stats_columns(
            None,
            configuration.unwrap_or_default(),
        )),
    }
}

#[pymodule]
// module name need to match project name
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    use crate::error::{CommitFailedError, DeltaError, SchemaMismatchError, TableNotFoundError};
    deltalake::aws::register_handlers(None);
    deltalake::azure::register_handlers(None);
    deltalake::gcp::register_handlers(None);
    deltalake::hdfs::register_handlers(None);
    deltalake_mount::register_handlers(None);
    deltalake::lakefs::register_handlers(None);
    deltalake::unity_catalog::register_handlers(None);

    init_client_version(format!("py-{}", env!("CARGO_PKG_VERSION")).as_str());

    let py = m.py();
    m.add("DeltaError", py.get_type::<DeltaError>())?;
    m.add("CommitFailedError", py.get_type::<CommitFailedError>())?;
    m.add("DeltaProtocolError", py.get_type::<DeltaProtocolError>())?;
    m.add("TableNotFoundError", py.get_type::<TableNotFoundError>())?;
    m.add("SchemaMismatchError", py.get_type::<SchemaMismatchError>())?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(pyo3::wrap_pyfunction!(rust_core_version, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(create_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(create_table_with_add_actions, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(write_to_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(convert_to_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        get_num_idx_cols_and_stats_columns,
        m
    )?)?;
    m.add_class::<RawDeltaTable>()?;
    m.add_class::<PyMergeBuilder>()?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_class::<RawDeltaTableMetaData>()?;
    m.add_class::<PyTransaction>()?;
    // There are issues with submodules, so we will expose them flat for now
    // See also: https://github.com/PyO3/pyo3/issues/759
    m.add_class::<schema::PrimitiveType>()?;
    m.add_class::<schema::ArrayType>()?;
    m.add_class::<schema::MapType>()?;
    m.add_class::<schema::Field>()?;
    m.add_class::<schema::StructType>()?;
    m.add_class::<schema::PySchema>()?;
    m.add_class::<filesystem::DeltaFileSystemHandler>()?;
    m.add_class::<filesystem::ObjectInputFile>()?;
    m.add_class::<filesystem::ObjectOutputStream>()?;
    m.add_class::<features::TableFeatures>()?;
    Ok(())
}
