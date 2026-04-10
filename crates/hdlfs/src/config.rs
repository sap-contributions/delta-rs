use std::collections::{hash_map::Entry, HashMap};
use std::str::FromStr;
use std::sync::LazyLock;

use crate::error::Result;
use object_store::hdlfs::SAPHdlfsConfigKey;
use object_store::Error as ObjectStoreError;

static CREDENTIAL_KEYS: LazyLock<Vec<SAPHdlfsConfigKey>> = LazyLock::new(|| {
    Vec::from_iter([
        SAPHdlfsConfigKey::PrivateKey,
        SAPHdlfsConfigKey::Certificate,
    ])
});

/// Credential
enum SAPHdlfsCredential {
    /// Using the service account key
    PrivateKey,
    /// Using application credentials
    Certificate,
}

impl SAPHdlfsCredential {
    /// required configuration keys for variant
    fn keys(&self) -> Vec<SAPHdlfsConfigKey> {
        match self {
            Self::PrivateKey => Vec::from_iter([SAPHdlfsConfigKey::PrivateKey]),
            Self::Certificate => Vec::from_iter([SAPHdlfsConfigKey::Certificate]),
        }
    }
}

/// Helper struct to create full configuration from passed options and environment
///
/// Main concern is to pick the desired credential for connecting to storage backend
/// based on a provided configuration and configuration set in the environment.
pub(crate) struct SAPHdlfsConfigHelper {
    config: HashMap<SAPHdlfsConfigKey, String>,
    env_config: HashMap<SAPHdlfsConfigKey, String>,
    priority: Vec<SAPHdlfsCredential>,
}

impl SAPHdlfsConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<Self> {
        let mut env_config = HashMap::new();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("SAP_") {
                    if let Ok(config_key) = SAPHdlfsConfigKey::from_str(&key.to_ascii_lowercase()) {
                        env_config.insert(config_key, value.to_string());
                    }
                }
            }
        }

        Ok(Self {
            config: config
                .into_iter()
                .map(|(key, value)| Ok((SAPHdlfsConfigKey::from_str(key.as_ref())?, value.into())))
                .collect::<Result<_, ObjectStoreError>>()?,
            env_config,
            priority: Vec::from_iter([
                SAPHdlfsCredential::PrivateKey,
                SAPHdlfsCredential::Certificate,
            ]),
        })
    }

    /// Check if all credential keys are contained in passed config
    fn has_full_config(&self, cred: &SAPHdlfsCredential) -> bool {
        cred.keys().iter().all(|key| self.config.contains_key(key))
    }

    /// Check if any credential keys are contained in passed config
    fn has_any_config(&self, cred: &SAPHdlfsCredential) -> bool {
        cred.keys().iter().any(|key| self.config.contains_key(key))
    }

    /// Check if all credential keys can be provided using the env
    fn has_full_config_with_env(&self, cred: &SAPHdlfsCredential) -> bool {
        cred.keys()
            .iter()
            .all(|key| self.config.contains_key(key) || self.env_config.contains_key(key))
    }

    /// Generate a configuration augmented with options from the environment
    pub fn build(mut self) -> Result<HashMap<SAPHdlfsConfigKey, String>> {
        let mut has_credential = false;

        // try using only passed config options
        if !has_credential {
            for cred in &self.priority {
                if self.has_full_config(cred) {
                    has_credential = true;
                    break;
                }
            }
        }

        // try partially available credentials augmented by environment
        if !has_credential {
            for cred in &self.priority {
                if self.has_any_config(cred) && self.has_full_config_with_env(cred) {
                    for key in cred.keys() {
                        if let Entry::Vacant(e) = self.config.entry(key) {
                            e.insert(self.env_config.get(&key).unwrap().to_owned());
                        }
                    }
                    has_credential = true;
                    break;
                }
            }
        }

        // try getting credentials only from the environment
        if !has_credential {
            for cred in &self.priority {
                if self.has_full_config_with_env(cred) {
                    for key in cred.keys() {
                        if let Entry::Vacant(e) = self.config.entry(key) {
                            e.insert(self.env_config.get(&key).unwrap().to_owned());
                        }
                    }
                    has_credential = true;
                    break;
                }
            }
        }

        let omit_keys = if has_credential {
            CREDENTIAL_KEYS.clone()
        } else {
            Vec::new()
        };

        // Add keys from the environment to the configuration, as e.g. client configuration options.
        // NOTE We have to specifically configure omitting keys, since workload identity can
        // work purely using defaults, but partial config may be present in the environment.
        // Preference of conflicting configs (e.g. msi resource id vs. client id is handled in object store)
        for key in self.env_config.keys() {
            if !omit_keys.contains(key) {
                if let Entry::Vacant(e) = self.config.entry(*key) {
                    e.insert(self.env_config.get(key).unwrap().to_owned());
                }
            }
        }

        Ok(self.config)
    }
}
