//! TOML config + `${ENV}` expansion + profile resolution.
//!
//! Schema (see DESIGN.md):
//!
//! ```toml
//! default_profile = "local"
//!
//! [profiles.local]
//! brokers = "localhost:9092"
//!
//! [profiles.prod]
//! brokers = "kafka-prod-1:9092,kafka-prod-2:9092"
//! sasl_mechanism = "PLAIN"
//! sasl_username = "${KQR_PROD_USER}"
//! sasl_password = "${KQR_PROD_PASS}"
//! schema_registry_url = "http://schema-registry:8081"
//!
//! [cache]
//! ttl = "1h"
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};

/// Top-level config loaded from `config.toml`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default = "default_profile_name")]
    pub default_profile: String,
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
    #[serde(default)]
    pub cache: CacheConfig,
}

fn default_profile_name() -> String {
    "default".to_string()
}

/// One named connection profile.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Profile {
    pub brokers: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub schema_registry_url: Option<String>,
}

/// Parquet cache settings.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// TTL as a humantime string (e.g. `"1h"`, `"30m"`). Default: `"1h"`.
    #[serde(default = "default_ttl_str")]
    pub ttl: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: default_ttl_str(),
        }
    }
}

fn default_ttl_str() -> String {
    "1h".to_string()
}

impl CacheConfig {
    pub fn ttl_duration(&self) -> Result<Duration> {
        humantime::parse_duration(&self.ttl).map_err(|e| Error::ConfigDuration(self.ttl.clone(), e))
    }
}

impl Config {
    /// Load config from an explicit path.
    pub fn load_from(path: &Path) -> Result<Self> {
        let bytes = std::fs::read_to_string(path).map_err(|source| Error::ConfigIo {
            path: path.to_path_buf(),
            source,
        })?;
        let cfg: Config = toml::from_str(&bytes)?;
        Ok(cfg)
    }

    /// Load from `~/.config/kqr/config.toml` if present, else return default.
    pub fn load_default() -> Result<Self> {
        let path = default_config_path()?;
        if path.exists() {
            Self::load_from(&path)
        } else {
            Ok(Self::default())
        }
    }

    /// Resolve a profile by name (or by `default_profile`), expanding `${ENV}`
    /// placeholders in string fields.
    pub fn select_profile(&self, name: Option<&str>) -> Result<Profile> {
        let resolved_name = name.unwrap_or(&self.default_profile).to_string();
        let mut profile = self
            .profiles
            .get(&resolved_name)
            .cloned()
            .ok_or(Error::ConfigProfileMissing(resolved_name))?;
        expand_env_in_profile(&mut profile)?;
        Ok(profile)
    }
}

/// `~/.config/kqr/config.toml` (XDG-ish, also works on macOS/Windows via [`dirs`]).
pub fn default_config_path() -> Result<PathBuf> {
    let base = dirs::config_dir().ok_or(Error::ConfigHomeUnavailable)?;
    Ok(base.join("kqr").join("config.toml"))
}

fn expand_env_in_profile(p: &mut Profile) -> Result<()> {
    p.brokers = expand_env(&p.brokers)?;
    if let Some(v) = p.sasl_mechanism.as_mut() {
        *v = expand_env(v)?;
    }
    if let Some(v) = p.sasl_username.as_mut() {
        *v = expand_env(v)?;
    }
    if let Some(v) = p.sasl_password.as_mut() {
        *v = expand_env(v)?;
    }
    if let Some(v) = p.schema_registry_url.as_mut() {
        *v = expand_env(v)?;
    }
    Ok(())
}

/// Replace `${VAR}` in `input` with `std::env::var("VAR")`. Returns
/// [`Error::ConfigEnvMissing`] if the var is unset, [`Error::ConfigEnvSyntax`]
/// if a `${` is unterminated.
pub fn expand_env(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut name = String::new();
            let mut closed = false;
            for nc in chars.by_ref() {
                if nc == '}' {
                    closed = true;
                    break;
                }
                name.push(nc);
            }
            if !closed || name.is_empty() {
                return Err(Error::ConfigEnvSyntax);
            }
            let value = std::env::var(&name).map_err(|_| Error::ConfigEnvMissing(name.clone()))?;
            out.push_str(&value);
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let toml = r#"
            default_profile = "local"
            [profiles.local]
            brokers = "localhost:9092"
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.default_profile, "local");
        assert_eq!(cfg.profiles["local"].brokers, "localhost:9092");
    }

    #[test]
    fn select_profile_default() {
        let toml = r#"
            default_profile = "x"
            [profiles.x]
            brokers = "h:9092"
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        let p = cfg.select_profile(None).unwrap();
        assert_eq!(p.brokers, "h:9092");
    }

    #[test]
    fn select_profile_explicit() {
        let toml = r#"
            default_profile = "x"
            [profiles.x]
            brokers = "x:9092"
            [profiles.y]
            brokers = "y:9092"
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        let p = cfg.select_profile(Some("y")).unwrap();
        assert_eq!(p.brokers, "y:9092");
    }

    #[test]
    fn select_profile_missing_errors() {
        let cfg: Config = toml::from_str(r#"default_profile = "x""#).unwrap();
        let err = cfg.select_profile(None).unwrap_err();
        assert!(matches!(err, Error::ConfigProfileMissing(ref n) if n == "x"));
    }

    #[test]
    fn env_expansion_replaces_var() {
        std::env::set_var("KQR_TEST_USER_VAR", "hunter2");
        assert_eq!(expand_env("u=${KQR_TEST_USER_VAR};").unwrap(), "u=hunter2;");
    }

    #[test]
    fn env_expansion_missing_errors() {
        std::env::remove_var("KQR_TEST_DEFINITELY_UNSET");
        let err = expand_env("${KQR_TEST_DEFINITELY_UNSET}").unwrap_err();
        assert!(matches!(err, Error::ConfigEnvMissing(_)));
    }

    #[test]
    fn env_expansion_unterminated_errors() {
        let err = expand_env("oops ${UNCLOSED").unwrap_err();
        assert!(matches!(err, Error::ConfigEnvSyntax));
    }

    #[test]
    fn env_expansion_no_dollar_passthrough() {
        assert_eq!(expand_env("plain text $bare").unwrap(), "plain text $bare");
    }

    #[test]
    fn select_profile_expands_env() {
        std::env::set_var("KQR_TEST_PASS", "s3cret");
        let toml = r#"
            default_profile = "p"
            [profiles.p]
            brokers = "h:9092"
            sasl_password = "${KQR_TEST_PASS}"
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        let prof = cfg.select_profile(None).unwrap();
        assert_eq!(prof.sasl_password.as_deref(), Some("s3cret"));
    }

    #[test]
    fn cache_ttl_default_one_hour() {
        let cfg = CacheConfig::default();
        assert_eq!(cfg.ttl_duration().unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn cache_ttl_custom() {
        let cfg = CacheConfig {
            ttl: "30m".to_string(),
        };
        assert_eq!(cfg.ttl_duration().unwrap(), Duration::from_secs(1800));
    }

    #[test]
    fn load_from_tempfile() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(
            &path,
            r#"
default_profile = "x"
[profiles.x]
brokers = "h:9092"
"#,
        )
        .unwrap();
        let cfg = Config::load_from(&path).unwrap();
        assert_eq!(cfg.default_profile, "x");
    }
}
