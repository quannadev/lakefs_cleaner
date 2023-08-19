use crate::duckdb_utils::setup_s3;
use config::{Config, Environment};
use lakefs::Config as LakeFsConfig;
use regex::Regex;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct FileConfig {
    pub size: u64,
    pub count: u64,
    pub branch: String,
    pub to_branch: Option<String>,
    pub repo: String,
    pub key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CleanerConfig {
    pub db_path: String,
    pub lakefs: LakeFsConfig,
    pub file_conf: FileConfig,
}

impl CleanerConfig {
    pub fn new() -> Result<Self, String> {
        let cfg = Config::builder()
            .set_default("db_path", "./data/lakefs.db")
            .map_err(|e| format!("Error setting default config: {}", e))?
            .add_source(Environment::default().prefix("FILE"))
            .add_source(Environment::default())
            .build()
            .map_err(|e| format!("Error reading config: {}", e))?;
        let db_path = cfg.clone().get("db_path").unwrap();
        let lakefs_cfg = cfg
            .clone()
            .try_deserialize::<LakeFsConfig>()
            .map_err(|e| format!("Error parsing config: {}", e))?;
        let file_cfg = cfg
            .try_deserialize::<FileConfig>()
            .map_err(|e| format!("Error parsing config: {}", e))?;
        Ok(Self {
            db_path,
            lakefs: lakefs_cfg,
            file_conf: file_cfg,
        })
    }
    pub fn set_db_s3(&self) -> String {
        let s3_access_key = self.lakefs.lakefs_access_key.clone();
        let s3_secret_key = self.lakefs.lakefs_secret_key.clone();
        let regex_pattern = Regex::new(r"https?://").unwrap();
        let s3_endpoint = regex_pattern
            .replace(&self.lakefs.lakefs_endpoint, "")
            .to_string();
        setup_s3(s3_endpoint, s3_access_key, s3_secret_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn set_env() {
        //env lakefs
        env::set_var("LAKEFS_ENDPOINT", "localhost:8000");
        env::set_var("LAKEFS_ACCESS_KEY", "AKIAJZ6FICUOBFLVMQJQ");
        env::set_var(
            "LAKEFS_SECRET_KEY",
            "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",
        );
        env::set_var("LAKEFS_API_VERSION", "v1");
        //env file
        env::set_var("FILE_SIZE", "1024");
        env::set_var("FILE_COUNT", "100");
        env::set_var("FILE_PREFIX", "test");
        env::set_var("FILE_BRANCH", "main");
        env::set_var("FILE_REPO", "ethereum");
        env::set_var("DB_PATH", "./data/lakefs.db");
    }

    #[test]
    //test config from env lakefs
    fn test_conf_lakefs() {
        set_env();
        let cfg = Config::builder()
            .add_source(Environment::default())
            .build()
            .unwrap();
        let lakefs_cfg = cfg.try_deserialize::<LakeFsConfig>().unwrap();
        assert_eq!(lakefs_cfg.lakefs_endpoint, "localhost:8000");
        assert_eq!(lakefs_cfg.lakefs_access_key, "AKIAJZ6FICUOBFLVMQJQ");
        assert_eq!(
            lakefs_cfg.lakefs_secret_key,
            "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"
        );
        assert_eq!(lakefs_cfg.lakefs_api_version, "v1");
    }

    #[test]
    fn test_config() {
        set_env();
        let cfg = CleanerConfig::new();
        println!("{:?}", cfg);
        let cfg = cfg.unwrap();
        assert_eq!(cfg.lakefs.lakefs_endpoint, "localhost:8000");
        assert_eq!(cfg.lakefs.lakefs_access_key, "AKIAJZ6FICUOBFLVMQJQ");
        assert_eq!(
            cfg.lakefs.lakefs_secret_key,
            "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"
        );
        assert_eq!(cfg.lakefs.lakefs_api_version, "v1");
        assert_eq!(cfg.file_conf.size, 1024);
        assert_eq!(cfg.file_conf.count, 100);
        assert_eq!(cfg.file_conf.branch, "main");
        assert_eq!(cfg.db_path, "./data/lakefs.db");
    }
}
