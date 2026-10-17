use crate::credentials::Credentials;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    pub endpoint: String,
    pub bucket: String,
    pub stand: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub version: u32,
    pub stand: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("version", &self.version)
            .field("stand", &self.stand)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl Config {
    pub fn profile(&self) -> Profile {
        Profile {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            stand: self.stand.clone(),
        }
    }

    pub fn prefix(&self) -> String {
        prefix_for(&self.stand)
    }

    pub fn keys(&self) -> Credentials {
        Credentials {
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
        }
    }
}

pub fn prefix_for(stand: &str) -> String {
    format!("stand-{stand}/")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    version: u32,
    stand: String,
    #[serde(default)]
    access_key: Option<String>,
    #[serde(default)]
    secret_key: Option<String>,
    s3: RawS3,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawS3 {
    endpoint: String,
    region: String,
    bucket: String,
    #[serde(default)]
    access_key: Option<String>,
    #[serde(default)]
    secret_key: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("нет файла config.toml рядом с программой. Положите config.toml в папку с sync.exe")]
    Missing,
    #[error("config.toml: {0}")]
    Invalid(String),
    #[error("config.toml не читается: {0}")]
    Io(#[from] std::io::Error),
}

pub fn load_config(workdir: &Path) -> Result<Config, ConfigError> {
    let path = workdir.join("config.toml");
    if !path.is_file() {
        return Err(ConfigError::Missing);
    }
    let text = fs::read_to_string(&path)?;
    if text.starts_with('\u{feff}') {
        return Err(ConfigError::Invalid(
            "уберите BOM: файл должен быть в UTF-8".into(),
        ));
    }
    parse_config(&text)
}

pub fn parse_config(text: &str) -> Result<Config, ConfigError> {
    let raw: RawConfig = toml::from_str(text).map_err(|err| {
        // message() не включает строку файла: в ней может быть ключ.
        let message = err.message();
        if message.contains("missing field")
            && (message.contains("access_key") || message.contains("secret_key"))
        {
            ConfigError::Invalid(
                "нужны access_key и secret_key: рядом с stand или в секции [s3]".into(),
            )
        } else {
            ConfigError::Invalid(message.to_string())
        }
    })?;
    if raw.version != 1 {
        return Err(ConfigError::Invalid(format!(
            "поддерживается только version = 1, в файле {}",
            raw.version
        )));
    }
    validate_stand(&raw.stand).map_err(ConfigError::Invalid)?;
    let endpoint = normalize_endpoint(&raw.s3.endpoint).map_err(ConfigError::Invalid)?;
    let region = raw.s3.region.trim().to_string();
    if region.is_empty() || region.chars().any(char::is_whitespace) {
        return Err(ConfigError::Invalid(
            "region пустой или содержит пробелы".into(),
        ));
    }
    validate_bucket(&raw.s3.bucket).map_err(ConfigError::Invalid)?;
    let access_key = pick_secret("access_key", raw.access_key, raw.s3.access_key)?;
    let secret_key = pick_secret("secret_key", raw.secret_key, raw.s3.secret_key)?;
    Ok(Config {
        version: raw.version,
        stand: raw.stand,
        endpoint,
        region,
        bucket: raw.s3.bucket,
        access_key,
        secret_key,
    })
}

fn pick_secret(
    name: &str,
    at_root: Option<String>,
    in_s3: Option<String>,
) -> Result<String, ConfigError> {
    let chosen = [at_root, in_s3]
        .into_iter()
        .flatten()
        .find(|value| !value.trim().is_empty());
    match chosen {
        Some(value) => validate_secret(name, &value),
        None => Err(ConfigError::Invalid(format!(
            "{name} пустой: напишите его рядом с stand или в секции [s3]"
        ))),
    }
}

fn validate_secret(name: &str, raw: &str) -> Result<String, ConfigError> {
    let value = raw.trim();
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::Invalid(format!(
            "{name} пустой или содержит пробелы"
        )));
    }
    Ok(value.to_string())
}

pub fn validate_stand(stand: &str) -> Result<(), String> {
    let len = stand.chars().count();
    if !(1..=63).contains(&len) {
        return Err(stand_message());
    }
    let bytes = stand.as_bytes();
    if bytes.len() != len {
        return Err(stand_message());
    }
    let first = bytes[0];
    let last = bytes[len - 1];
    if !is_alnum(first) || !is_alnum(last) {
        return Err(stand_message());
    }
    if !bytes.iter().all(|b| is_alnum(*b) || *b == b'-') {
        return Err(stand_message());
    }
    Ok(())
}

fn stand_message() -> String {
    "stand: 1..63 символа, только строчные латинские буквы, цифры и дефис; первый и последний символ не дефис"
        .into()
}

fn is_alnum(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit()
}

fn normalize_endpoint(raw: &str) -> Result<String, String> {
    let endpoint = raw.trim().trim_end_matches('/').to_string();
    if endpoint.is_empty() {
        return Err("endpoint пустой".into());
    }
    let lower = endpoint.to_ascii_lowercase();
    if !(lower.starts_with("https://") || lower.starts_with("http://")) {
        return Err("endpoint должен начинаться с https://".into());
    }
    if endpoint.contains('@') || endpoint.contains(' ') {
        return Err("endpoint не должен содержать ключи или пробелы".into());
    }
    Ok(endpoint)
}

fn validate_bucket(bucket: &str) -> Result<(), String> {
    if bucket.is_empty()
        || bucket.len() > 255
        || bucket.contains('/')
        || bucket.contains('\\')
        || bucket.contains(' ')
        || bucket.contains("..")
    {
        return Err("некорректное имя бакета".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn dayna_prefix() {
        assert_eq!(prefix_for("dayna"), "stand-dayna/");
    }

    #[test]
    fn stand_rules() {
        assert!(validate_stand("dayna").is_ok());
        assert!(validate_stand("a").is_ok());
        assert!(validate_stand("a-b").is_ok());
        assert!(validate_stand("a--b").is_ok());
        assert!(validate_stand(&"a".repeat(63)).is_ok());
        assert!(validate_stand("").is_err());
        assert!(validate_stand("..").is_err());
        assert!(validate_stand("a/b").is_err());
        assert!(validate_stand("Dayna").is_err());
        assert!(validate_stand("-dayna").is_err());
        assert!(validate_stand("dayna-").is_err());
        assert!(validate_stand(&"a".repeat(64)).is_err());
        assert!(validate_stand("день").is_err());
    }

    #[test]
    fn sample_config_parses() {
        let cfg = parse_config(include_str!("../config.example.toml")).unwrap();
        assert_eq!(cfg.stand, "dayna");
        assert_eq!(cfg.region, "ru-1");
        assert_eq!(cfg.endpoint, "https://s3.twcstorage.ru");
        assert_eq!(cfg.prefix(), "stand-dayna/");
        assert_eq!(cfg.bucket, "13820aae-0a3c-463d-b672-d25f73eda26a");
        assert_eq!(cfg.access_key, "AKIA_EXAMPLE_KEY");
        assert!(!format!("{cfg:?}").contains("secret-example-key"));
        assert!(!format!("{:?}", cfg.keys()).contains("secret-example-key"));
    }

    #[test]
    fn keys_at_the_root_are_accepted_and_parse_errors_omit_the_line() {
        let text = r#"
version = 1
stand = "dayna"
access_key = "AKIA_EXAMPLE_KEY"
secret_key = "secret-example-key"
[s3]
endpoint = "https://s3.twcstorage.ru"
region = "ru-1"
bucket = "bucket"
"#;
        let cfg = parse_config(text).unwrap();
        assert_eq!(cfg.access_key, "AKIA_EXAMPLE_KEY");
        assert_eq!(cfg.secret_key, "secret-example-key");

        let nested = r#"
version = 1
stand = "dayna"
[s3]
endpoint = "https://s3.twcstorage.ru"
region = "ru-1"
bucket = "bucket"
access_key = "AKIA_EXAMPLE_KEY"
secret_key = "secret-example-key"
"#;
        assert_eq!(
            parse_config(nested).unwrap().secret_key,
            "secret-example-key"
        );

        let broken = r#"
version = 1
stand = "dayna"
access_key = "AKIA_EXAMPLE_KEY"
secret_key = "secret-example-key"
extra = "no"
[s3]
endpoint = "https://s3.twcstorage.ru"
region = "ru-1"
bucket = "bucket"
"#;
        let message = parse_config(broken).unwrap_err().to_string();
        assert!(!message.contains("AKIA_EXAMPLE_KEY"));
        assert!(!message.contains("secret-example-key"));
        assert!(message.contains("extra"));
    }

    #[test]
    fn empty_keys_are_a_local_error_and_are_not_echoed() {
        let text = r#"
version = 1
stand = "dayna"
[s3]
endpoint = "https://s3.twcstorage.ru"
region = "ru-1"
bucket = "bucket"
access_key = ""
secret_key = "secret-example-key"
"#;
        let err = parse_config(text).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("access_key"));
        assert!(!message.contains("secret-example-key"));
    }

    #[test]
    fn unknown_fields_and_bad_version_are_local_errors() {
        assert!(parse_config("version = 1\nstand = \"dayna\"\nprefix = \"x\"\n").is_err());
        let with_prefix = r#"
version = 1
stand = "dayna"
[s3]
endpoint = "https://s3.twcstorage.ru"
region = "ru-1"
bucket = "bucket"
prefix = "no"
"#;
        assert!(parse_config(with_prefix).is_err());
        assert!(parse_config(
            "version = 2\nstand = \"dayna\"\n[s3]\nendpoint = \"https://s3.twcstorage.ru\"\nregion = \"ru-1\"\nbucket = \"b\"\n"
        )
        .is_err());
        assert!(parse_config("stand = \"Dayna\"\nversion = 1\n[s3]\nendpoint = \"https://s3.twcstorage.ru\"\nregion = \"ru-1\"\nbucket = \"b\"\n").is_err());
    }

    #[test]
    fn missing_file_does_not_invent_a_profile() {
        let dir = crate::testutil::temp_dir("cfg-missing");
        let err = load_config(&dir).unwrap_err();
        assert!(matches!(err, ConfigError::Missing));
    }

    #[test]
    fn load_does_not_rewrite_config() {
        let dir = crate::testutil::temp_dir("cfg-readonly");
        let path = dir.join("config.toml");
        let body = include_str!("../config.example.toml");
        fs::write(&path, body).unwrap();
        let before = fs::read(&path).unwrap();
        load_config(&dir).unwrap();
        assert_eq!(fs::read(&path).unwrap(), before);
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"").unwrap();
    }
}
