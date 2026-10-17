use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Credentials { .. }")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CredError {
    message: String,
}

impl CredError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn account_id(endpoint: &str, bucket: &str, stand: &str) -> String {
    format!("v1\u{1f}{endpoint}\u{1f}{bucket}\u{1f}{stand}")
}

pub trait SecretStore: Send + Sync {
    fn load(&self, account: &str) -> Result<Option<Credentials>, CredError>;
    fn save(&self, account: &str, creds: &Credentials) -> Result<(), CredError>;
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<HashMap<String, String>>,
}

impl SecretStore for MemoryStore {
    fn load(&self, account: &str) -> Result<Option<Credentials>, CredError> {
        let guard = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        match guard.get(account) {
            Some(raw) => decode(raw).map(Some),
            None => Ok(None),
        }
    }

    fn save(&self, account: &str, creds: &Credentials) -> Result<(), CredError> {
        let raw = encode(creds)?;
        let mut guard = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        guard.insert(account.to_string(), raw);
        Ok(())
    }
}

#[cfg(windows)]
pub struct KeyringStore;

#[cfg(windows)]
impl SecretStore for KeyringStore {
    fn load(&self, account: &str) -> Result<Option<Credentials>, CredError> {
        let entry = keyring::Entry::new("stand-sync", account)
            .map_err(|err| CredError::new(format!("диспетчер учётных данных: {err}")))?;
        match entry.get_password() {
            Ok(raw) => decode(&raw).map(Some),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(err) => Err(CredError::new(format!("диспетчер учётных данных: {err}"))),
        }
    }

    fn save(&self, account: &str, creds: &Credentials) -> Result<(), CredError> {
        let entry = keyring::Entry::new("stand-sync", account)
            .map_err(|err| CredError::new(format!("диспетчер учётных данных: {err}")))?;
        let raw = encode(creds)?;
        entry
            .set_password(&raw)
            .map_err(|err| CredError::new(format!("диспетчер учётных данных: {err}")))?;
        Ok(())
    }
}

fn encode(creds: &Credentials) -> Result<String, CredError> {
    serde_json::to_string(creds).map_err(|err| CredError::new(err.to_string()))
}

fn decode(raw: &str) -> Result<Credentials, CredError> {
    serde_json::from_str(raw)
        .map_err(|_| CredError::new("сохранённые ключи повреждены".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triple_is_the_account_key() {
        let a = account_id("https://s3.twcstorage.ru", "bucket", "dayna");
        let b = account_id("https://s3.twcstorage.ru", "bucket", "other");
        assert_ne!(a, b);
        assert_eq!(a, account_id("https://s3.twcstorage.ru", "bucket", "dayna"));
    }

    #[test]
    fn memory_roundtrip_keeps_keys_out_of_debug() {
        let store = MemoryStore::default();
        let creds = Credentials {
            access_key: "access-key-value".into(),
            secret_key: "secret-key-value".into(),
        };
        let account = account_id("https://example", "b", "dayna");
        store.save(&account, &creds).unwrap();
        assert_eq!(store.load(&account).unwrap().unwrap(), creds);
        assert!(format!("{creds:?}").contains(".."));
        assert!(!format!("{creds:?}").contains("secret-key-value"));
    }

    #[test]
    fn credential_module_does_not_log() {
        let src = include_str!("credentials.rs");
        let src = src.split("#[cfg(test)]").next().unwrap();
        assert!(!src.contains("tracing"));
        assert!(!src.contains("println"));
        assert!(!src.contains("eprintln"));
        assert!(!src.contains("config.toml"));
    }
}
