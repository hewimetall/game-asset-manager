pub const MIB: u64 = 1024 * 1024;
pub const PART_SIZE: u64 = 5 * MIB;
pub const MULTIPART_THRESHOLD: u64 = 8 * MIB;
pub const MAX_PARTS: u64 = 10_000;
pub const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024 * 1024;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SizeError {
    #[error("файл больше 50 ГиБ ({size} байт). Размер части не увеличивается")]
    OverLimit { size: u64 },
    #[error(
        "файл {size} байт требует больше 10000 частей по 5 МиБ. Размер части не увеличивается"
    )]
    TooManyParts { size: u64, parts: u64 },
}

pub fn part_count(size: u64) -> u64 {
    if size == 0 {
        return 1;
    }
    size.div_ceil(PART_SIZE)
}

pub fn check_upload_size(size: u64) -> Result<(), SizeError> {
    if size > MAX_FILE_BYTES {
        return Err(SizeError::OverLimit { size });
    }
    if size >= MULTIPART_THRESHOLD {
        let parts = part_count(size);
        if parts > MAX_PARTS {
            return Err(SizeError::TooManyParts { size, parts });
        }
    }
    Ok(())
}

pub fn use_multipart(size: u64) -> bool {
    size >= MULTIPART_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds_stay_fixed() {
        assert_eq!(PART_SIZE, 5 * 1024 * 1024);
        assert_eq!(MULTIPART_THRESHOLD, 8 * 1024 * 1024);
        assert_eq!(MAX_PARTS, 10_000);
        assert_eq!(MAX_FILE_BYTES, 50 * 1024 * 1024 * 1024);
        assert!(!use_multipart(MULTIPART_THRESHOLD - 1));
        assert!(use_multipart(MULTIPART_THRESHOLD));
    }

    #[test]
    fn oversized_files_are_rejected_without_growing_parts() {
        assert!(
            check_upload_size(MAX_FILE_BYTES).is_err() || part_count(MAX_FILE_BYTES) > MAX_PARTS
        );
        assert!(matches!(
            check_upload_size(MAX_FILE_BYTES + 1),
            Err(SizeError::OverLimit { .. })
        ));
        let almost = MAX_PARTS * PART_SIZE + 1;
        if almost <= MAX_FILE_BYTES {
            assert!(matches!(
                check_upload_size(almost),
                Err(SizeError::TooManyParts { .. })
            ));
        }
        assert!(check_upload_size(PART_SIZE * 3).is_ok());
        assert_eq!(PART_SIZE, 5 * MIB);
    }
}
