use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Clean a server-provided or user-provided file name so it can never escape
/// the download directory or break the filesystem.
pub fn sanitize_file_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let mut cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        .collect();
    while cleaned.ends_with(['.', ' ']) {
        cleaned.pop();
    }
    if cleaned.len() > 200 {
        cleaned.truncate(200);
    }
    let stem = cleaned.split('.').next().unwrap_or("").to_ascii_uppercase();
    if cleaned.is_empty()
        || cleaned == "."
        || cleaned == ".."
        || RESERVED_NAMES.contains(&stem.as_str())
    {
        cleaned = format!("_{cleaned}");
    }
    if cleaned.is_empty() || cleaned == "_" {
        cleaned = "download".to_string();
    }
    cleaned
}

/// Pick a path in `dir` that does not collide with an existing file,
/// appending " (n)" before the extension when needed.
pub fn unique_path(dir: &Path, file_name: &str) -> PathBuf {
    let candidate = dir.join(file_name);
    if !candidate.exists() {
        return candidate;
    }
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), format!(".{e}")),
        _ => (file_name.to_string(), String::new()),
    };
    for n in 1..10_000 {
        let candidate = dir.join(format!("{stem} ({n}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}.{}{ext}", now_unix_nanos()))
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Exponential backoff with up to 25% jitter, capped at `max_delay_ms`.
pub fn backoff_delay(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> Duration {
    let exp = attempt.saturating_sub(1).min(16);
    let raw = base_delay_ms.saturating_mul(1u64 << exp);
    let capped = raw.min(max_delay_ms);
    let jitter = (now_unix_nanos() % 1000) as u64;
    let with_jitter = capped + capped / 4 * jitter / 1000;
    Duration::from_millis(with_jitter.min(max_delay_ms.saturating_mul(2)))
}

/// Parse a Retry-After header value given in delta-seconds.
/// HTTP-date values are not supported and yield `None`.
pub fn parse_retry_after(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_paths() {
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(
            sanitize_file_name(r"C:\Windows\System32\evil.dll"),
            "evil.dll"
        );
        assert_eq!(sanitize_file_name("a/b/c.txt"), "c.txt");
    }

    #[test]
    fn sanitize_forbidden_chars() {
        assert_eq!(sanitize_file_name("a:b*c?.txt"), "abc.txt");
        assert_eq!(sanitize_file_name("name. "), "name");
        assert_eq!(sanitize_file_name(""), "download");
        assert_eq!(sanitize_file_name(".."), "download");
    }

    #[test]
    fn sanitize_reserved_names() {
        assert_eq!(sanitize_file_name("CON"), "_CON");
        assert_eq!(sanitize_file_name("nul.txt"), "_nul.txt");
        assert_eq!(sanitize_file_name("normal.txt"), "normal.txt");
    }

    #[test]
    fn unique_path_avoids_collision() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("file.bin"), b"x").unwrap();
        let p = unique_path(dir.path(), "file.bin");
        assert_eq!(p.file_name().unwrap(), "file (1).bin");
        std::fs::write(&p, b"x").unwrap();
        let p2 = unique_path(dir.path(), "file.bin");
        assert_eq!(p2.file_name().unwrap(), "file (2).bin");
    }

    #[test]
    fn backoff_grows_and_caps() {
        let d1 = backoff_delay(1, 100, 10_000);
        let d4 = backoff_delay(4, 100, 10_000);
        assert!(d4 > d1);
        let d20 = backoff_delay(20, 100, 10_000);
        assert!(d20 <= Duration::from_millis(20_000));
    }

    #[test]
    fn retry_after_seconds() {
        assert_eq!(parse_retry_after("120"), Some(120));
        assert_eq!(parse_retry_after(" 5 "), Some(5));
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }
}
