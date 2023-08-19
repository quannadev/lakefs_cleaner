use config::{Config, Environment};
use serde::Deserialize;
use lakefs::Config as LakeFsConfig;

#[derive(Debug, Deserialize)]
pub struct TaskConfig {
    pub file_size: u64,
    pub file_count: u64,
    pub file_prefix: String,
    pub file_branch: String,
}

#[derive(Debug, Deserialize)]
pub struct CleanerConfig {
    pub lakefs: LakeFsConfig,
    pub task: TaskConfig,
}

impl CleanerConfig {
    pub fn new() -> Result<Self, String> {
        let cfg = Config::builder()
            .add_source(Environment::with_prefix("LAKEFS"))
            .add_source(Environment::with_prefix("FILE"))
            .build()
            .map_err(|e| format!("Error reading config: {}", e))?;
        let lakefs_cfg = cfg.try_deserialize::<Self>()
            .map_err(|e| format!("Error parsing config: {}", e))?;
        Ok(lakefs_cfg)

    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use super::*;

    fn set_env(){
        //env lakefs
        env::set_var("LAKEFS_ENDPOINT", "localhost:8000");
        env::set_var("LAKEFS_ACCESS_KEY", "AKIAJZ6FICUOBFLVMQJQ");
        env::set_var(
            "LAKEFS_SECRET_KEY",
            "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",
        );
        env::set_var("LAKEFS_API_VERSION", "v1");
        //env file
        env::set_var("FILE_FILE_SIZE", "1024");
        env::set_var("FILE_FILE_COUNT", "100");
        env::set_var("FILE_FILE_PREFIX", "test");
        env::set_var("FILE_FILE_BRANCH", "main");
    }
    #[test]
    fn test_config() {
        set_env();
        let cfg = CleanerConfig::new().unwrap();
        assert_eq!(cfg.lakefs.lakefs_endpoint, "localhost:8000");
        assert_eq!(cfg.lakefs.lakefs_access_key, "AKIAJZ6FICUOBFLVMQJQ");
        assert_eq!(
            cfg.lakefs.lakefs_secret_key,
            "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"
        );
        assert_eq!(cfg.lakefs.lakefs_api_version, "v1");
        assert_eq!(cfg.task.file_size, 1024);
        assert_eq!(cfg.task.file_count, 100);
        assert_eq!(cfg.task.file_prefix, "test");
        assert_eq!(cfg.task.file_branch, "main");
    }
}