use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{
    default_logstore, logstore_factories, object_store_factories, LogStore, LogStoreFactory,
    ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::hdlfs::{SAPHdlfsBuilder, SAPHdlfsConfigKey};
use object_store::{ObjectStoreScheme};
use url::Url;

mod config;
mod error;

trait HdlfsOptions {
    fn as_hdlfs_options(&self) -> HashMap<SAPHdlfsConfigKey, String>;
}

impl HdlfsOptions for HashMap<String, String> {
    fn as_hdlfs_options(&self) -> HashMap<SAPHdlfsConfigKey, String> {
        self.iter()
            .filter_map(|(key, value)| {
                Some((
                    SAPHdlfsConfigKey::from_str(&key.to_ascii_lowercase()).ok()?,
                    value.clone(),
                ))
            })
            .collect()
    }
}

#[derive(Clone, Default, Debug)]
pub struct HdlfsFactory {}

impl ObjectStoreFactory for HdlfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::SAPHdlfsConfigHelper::try_new(config.raw.as_hdlfs_options())?.build()?;

        //Stefan Ruck fix the issue: Enable usage of env variables for configuration.
        let mut builder = SAPHdlfsBuilder::from_env()
            .with_url(url.to_string());
        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }
        let store = builder.build()?;

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        Ok((Arc::new(store), prefix))
    }
}

impl LogStoreFactory for HdlfsFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register an [ObjectStoreFactory] for common Hdlfs [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    eprintln!(
        "{}",
        "Registering **** hdlfs ****** call register_handlers..."
    );

    let factory = Arc::new(HdlfsFactory {});
    let scheme = &"hdlfs";
    let url = Url::parse(&format!("{scheme}://")).unwrap();
    object_store_factories().insert(url.clone(), factory.clone());
    logstore_factories().insert(url.clone(), factory.clone());
}
