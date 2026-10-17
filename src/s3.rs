use crate::config::Profile;
use crate::credentials::Credentials as Keys;
use crate::store::{
    classify_http, Downloaded, HeadInfo, ListPage, ListedObject, ObjectStore, PartInfo, PartsPage,
    StoreError,
};
use async_trait::async_trait;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct S3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3Store {
    pub fn new(profile: &Profile, region: &str, keys: &Keys) -> Result<Self, String> {
        if profile.endpoint.is_empty() || profile.bucket.is_empty() || profile.stand.is_empty() {
            return Err("неполный профиль".into());
        }
        let creds = Credentials::new(
            keys.access_key.clone(),
            keys.secret_key.clone(),
            None,
            None,
            "stand-sync",
        );
        let timeout = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(300))
            .operation_attempt_timeout(Duration::from_secs(600))
            .build();
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(&profile.endpoint)
            .credentials_provider(creds)
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .retry_config(RetryConfig::standard().with_max_attempts(2))
            .timeout_config(timeout)
            .build();
        Ok(Self {
            client: aws_sdk_s3::Client::from_conf(config),
            bucket: profile.bucket.clone(),
            prefix: format!("stand-{}/", profile.stand),
        })
    }

    fn guard_prefix(&self, prefix: &str) -> Result<(), StoreError> {
        if prefix.is_empty() || !prefix.ends_with('/') || !prefix.starts_with("stand-") {
            return Err(StoreError::Refused("листинг без префикса запрещён".into()));
        }
        Ok(())
    }

    fn guard_key(&self, key: &str) -> Result<(), StoreError> {
        if self.prefix.is_empty() || !key.starts_with(&self.prefix) || key.contains("..") {
            return Err(StoreError::Refused("ключ вне префикса стенда".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn list_page(
        &self,
        prefix: &str,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<ListPage, StoreError> {
        self.guard_prefix(prefix)?;
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(max_keys.max(1));
        if let Some(token) = token {
            req = req.continuation_token(token);
        }
        let out = req.send().await.map_err(map_sdk)?;
        let mut objects = Vec::new();
        for item in out.contents() {
            let Some(key) = item.key() else { continue };
            let size = item.size().unwrap_or(0).max(0) as u64;
            let etag = item.e_tag().unwrap_or("").to_string();
            if etag.is_empty() {
                tracing::warn!(key, "объект без ETag пропущен");
                continue;
            }
            objects.push(ListedObject {
                key: key.to_string(),
                size,
                etag,
            });
        }
        let truncated = out.is_truncated().unwrap_or(false);
        let next_token = if truncated {
            out.next_continuation_token().map(str::to_string)
        } else {
            None
        };
        Ok(ListPage {
            objects,
            next_token,
            truncated,
        })
    }

    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<String, StoreError> {
        self.guard_key(key)?;
        let out = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(map_sdk)?;
        out.e_tag()
            .map(normalize_raw_etag)
            .ok_or_else(|| StoreError::Other {
                status: 200,
                code: "MissingETag".into(),
                request_id: None,
                message: "PutObject не вернул ETag".into(),
            })
    }

    async fn create_multipart(&self, key: &str) -> Result<String, StoreError> {
        self.guard_key(key)?;
        let out = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(map_sdk)?;
        out.upload_id()
            .map(str::to_string)
            .ok_or_else(|| StoreError::Other {
                status: 200,
                code: "MissingUploadId".into(),
                request_id: None,
                message: "CreateMultipartUpload не вернул upload id".into(),
            })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Vec<u8>,
    ) -> Result<String, StoreError> {
        self.guard_key(key)?;
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(map_sdk)?;
        out.e_tag()
            .map(normalize_raw_etag)
            .ok_or_else(|| StoreError::Other {
                status: 200,
                code: "MissingETag".into(),
                request_id: None,
                message: format!("часть {part_number} не вернула ETag"),
            })
    }

    async fn list_parts_page(
        &self,
        key: &str,
        upload_id: &str,
        marker: Option<i32>,
    ) -> Result<PartsPage, StoreError> {
        self.guard_key(key)?;
        let mut req = self
            .client
            .list_parts()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .max_parts(1000);
        if let Some(marker) = marker {
            req = req.part_number_marker(marker.to_string());
        }
        let out = req.send().await.map_err(map_sdk)?;
        let mut parts = Vec::new();
        for part in out.parts() {
            let Some(number) = part.part_number() else {
                continue;
            };
            let Some(etag) = part.e_tag() else { continue };
            let size = part.size().unwrap_or(0).max(0) as u64;
            parts.push(PartInfo {
                number,
                size,
                etag: normalize_raw_etag(etag),
            });
        }
        let truncated = out.is_truncated().unwrap_or(false);
        let next_marker = if truncated {
            out.next_part_number_marker()
                .and_then(|raw| raw.parse().ok())
        } else {
            None
        };
        Ok(PartsPage {
            parts,
            next_marker,
            truncated,
        })
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<String, StoreError> {
        self.guard_key(key)?;
        let mut ordered = parts;
        ordered.sort_by_key(|(number, _)| *number);
        let mut completed = Vec::with_capacity(ordered.len());
        for (number, etag) in ordered {
            completed.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(number)
                    .e_tag(etag)
                    .build(),
            );
        }
        let upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        let out = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(map_sdk)?;
        out.e_tag()
            .map(normalize_raw_etag)
            .ok_or_else(|| StoreError::Other {
                status: 200,
                code: "MissingETag".into(),
                request_id: None,
                message: "CompleteMultipartUpload не вернул ETag".into(),
            })
    }

    async fn head(&self, key: &str) -> Result<HeadInfo, StoreError> {
        self.guard_key(key)?;
        let out = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(map_sdk)?;
        let etag = out
            .e_tag()
            .map(normalize_raw_etag)
            .ok_or_else(|| StoreError::Other {
                status: 200,
                code: "MissingETag".into(),
                request_id: None,
                message: "HeadObject не вернул ETag".into(),
            })?;
        let size = out.content_length().unwrap_or(0).max(0) as u64;
        Ok(HeadInfo { size, etag })
    }

    async fn get_to(
        &self,
        key: &str,
        dest: &Path,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<Downloaded, StoreError> {
        self.guard_key(key)?;
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(map_sdk)?;
        let etag = out.e_tag().unwrap_or("").to_string();
        let expected = out.content_length().map(|len| len.max(0) as u64);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|err| StoreError::network(err.to_string()))?;
        }
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|err| StoreError::network(err.to_string()))?;
        let mut reader = out.body.into_async_read();
        let mut buf = vec![0u8; 64 * 1024];
        let mut written = 0u64;
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|err| StoreError::network(err.to_string()))?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])
                .await
                .map_err(|err| StoreError::network(err.to_string()))?;
            written += n as u64;
            progress(written);
        }
        file.sync_all()
            .await
            .map_err(|err| StoreError::network(err.to_string()))?;
        if let Some(expected) = expected {
            if written != expected {
                return Err(StoreError::Other {
                    status: 200,
                    code: "SizeMismatch".into(),
                    request_id: None,
                    message: format!("GET вернул {written} байт, Content-Length {expected}"),
                });
            }
        }
        Ok(Downloaded {
            bytes_written: written,
            etag: normalize_raw_etag(&etag),
        })
    }
}

fn normalize_raw_etag(raw: &str) -> String {
    crate::paths::normalize_etag(raw)
}

fn map_sdk<E>(err: SdkError<E>) -> StoreError
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    match &err {
        SdkError::ServiceError(service) => {
            let code = service.err().code().unwrap_or("").to_string();
            let message = service
                .err()
                .message()
                .map(str::to_string)
                .unwrap_or_else(|| service.err().to_string());
            let request_id = service.err().meta().request_id().map(str::to_string);
            let status = service.raw().status().as_u16();
            classify_http(status, &code, request_id, message)
        }
        SdkError::ResponseError(inner) => {
            let status = inner.raw().status().as_u16();
            classify_http(status, "", None, sdk_message(&err))
        }
        _ => StoreError::Network {
            message: sdk_message(&err),
            request_id: None,
        },
    }
}

fn sdk_message(err: &impl std::error::Error) -> String {
    let mut out = err.to_string();
    let mut current = err.source();
    for _ in 0..4 {
        let Some(next) = current else { break };
        let text = next.to_string();
        let lower = text.to_ascii_lowercase();
        if lower.contains("x-amz-")
            || lower.contains("signature=")
            || lower.contains("authorization")
        {
            break;
        }
        out.push_str(": ");
        out.push_str(&text);
        current = next.source();
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn client_stays_on_required_checksums_and_avoids_forbidden_apis() {
        let src = include_str!("s3.rs");
        let src = src.split("#[cfg(test)]").next().unwrap();
        assert!(src.contains("RequestChecksumCalculation::WhenRequired"));
        assert!(src.contains("force_path_style(true)"));
        assert!(!src.to_lowercase().contains("checksum_algorithm"));
        assert!(!src.contains("list_buckets"));
        assert!(!src.contains("head_bucket"));
        assert!(!src.contains("delete_object"));
        assert!(!src.contains("abort_multipart"));
        assert!(!src.contains(".delimiter("));
    }
}
