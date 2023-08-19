use crate::config::{CleanerConfig, FileConfig};
use crate::errors::CleanerError;
use duckdb::Connection;
use lakefs::{LakeFsClient, ObjectItem};
use std::sync::Arc;
use tokio::sync::Mutex;

type CleanerResult<T> = Result<T, CleanerError>;
#[derive(Debug)]
pub struct Cleaner {
    conn: Arc<Mutex<Connection>>,
    client: LakeFsClient,
    file_conf: FileConfig,
    table_name: String,
    current_branch: String,
}

impl Cleaner {
    pub fn new(cfg: &CleanerConfig) -> CleanerResult<Self> {
        let conn = Connection::open(cfg.db_path.clone()).unwrap();
        let setup_query = cfg.set_db_s3();
        log::info!("setup query: {}", setup_query);
        conn.execute_batch(&setup_query)
            .map_err(|e| CleanerError::Init(e.to_string()))?;
        let client = LakeFsClient::new(cfg.lakefs.clone());
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            client,
            file_conf: cfg.file_conf.clone(),
            table_name: cfg.file_conf.repo.clone(),
            current_branch: cfg.file_conf.branch.clone(),
        })
    }

    async fn run(&self) -> CleanerResult<()> {
        self.init_table_from_file().await?;
        let mut count = 0;
        while count < self.file_conf.count {
            let files = self
                .get_files_from_lakefs(self.file_conf.count + count)
                .await?;
            for file in files.iter() {
                self.insert_file_to_table(file.path.clone()).await?;
            }
            count += files.len() as u64;

            let files = self.get_files_from_lakefs(self.file_conf.count).await?;
            for file in files {
                self.insert_file_to_table(file.path).await?;
            }
            let file_name = format!("file_{}", count);
            self.copy_to_s3(file_name).await?;
            self.drop_table().await?;
        }

        Ok(())
    }

    async fn init_table_from_file(&self) -> CleanerResult<()> {
        let files = self.get_files_from_lakefs(1).await?;
        if files.is_empty() {
            return Err(CleanerError::Unknown("No files in lakefs".to_string()));
        }
        let file_name = files.first().unwrap().path.clone();
        let file_path = format!("s3://{}/{}", self.file_conf.repo.clone(), file_name);
        let conn = self.conn.lock().await;
        let table_name = self.table_name.clone();
        let query =
            format!("CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');");
        conn.execute(&query, [])?;
        Ok(())
    }

    async fn copy_to_s3(&self, file_name: String) -> CleanerResult<()> {
        let query = format!(
            "COPY {} TO '{}.parquet' (FORMAT 'PARQUET')",
            self.table_name, file_name
        );
        let conn = self.conn.lock().await;
        conn.execute(&query, [])?;
        Ok(())
    }

    async fn drop_table(&self) -> CleanerResult<()> {
        let conn = self.conn.lock().await;
        let table_name = self.table_name.clone();
        let query = format!("DROP TABLE {table_name};");
        conn.execute(&query, [])?;
        Ok(())
    }

    async fn insert_file_to_table(&self, file_name: String) -> CleanerResult<()> {
        let file_path = format!(
            "s3://{}/{}/{}",
            self.file_conf.repo.clone(),
            self.file_conf.branch,
            file_name
        );
        let conn = self.conn.lock().await;
        let table_name = self.file_conf.branch.clone();
        let query = format!("INSERT INTO {table_name} SELECT * FROM read_parquet('{file_path}');");
        conn.execute(&query, [])?;
        Ok(())
    }

    async fn get_file_by_name(&self, name: String) -> CleanerResult<ObjectItem> {
        let query = lakefs::QueryData {
            amount: 1,
            file_name: name,
            ..Default::default()
        };
        let result = self
            .client
            .object_api
            .get_file_obj(
                self.file_conf.repo.clone(),
                self.file_conf.branch.clone(),
                query,
            )
            .await
            .map_err(|e| CleanerError::Lakefs(e.to_string()))?;
        Ok(result)
    }

    async fn get_files_from_lakefs(&self, amount: u64) -> CleanerResult<Vec<ObjectItem>> {
        let query = lakefs::QueryData {
            amount: self.file_conf.count,
            ..Default::default()
        };
        let result = self
            .client
            .object_api
            .ls_objects(
                self.file_conf.repo.clone(),
                self.file_conf.branch.clone(),
                query,
            )
            .await
            .map_err(|e| CleanerError::Lakefs(e.to_string()))?;
        log::info!("Got {:?}", result);
        Ok(result.results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn set_env() {
        env::set_var("RUST_LOG", "INFO");
        env_logger::try_init().unwrap_or_default();
        //env lakefs
        env::set_var("LAKEFS_ENDPOINT", "https://lakefs.quanna.dev");
        env::set_var("LAKEFS_ACCESS_KEY", "AKIAJAJEXKPXEEUA6ZOQ");
        env::set_var(
            "LAKEFS_SECRET_KEY",
            "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",
        );
        env::set_var("LAKEFS_API_VERSION", "v1");
        //env file
        env::set_var("FILE_SIZE", "1024");
        env::set_var("FILE_COUNT", "100");
        env::set_var("FILE_BRANCH", "main");
        env::set_var("FILE_KEY", "block_number");
        env::set_var("FILE_TO_BRANCH", "test");
        env::set_var("FILE_REPO", "ethereum");

        env::set_var("DB_PATH", "./data/lakefs.db");
    }

    #[tokio::test]
    async fn get_list_file() {
        set_env();
        let cfg = CleanerConfig::new().unwrap();
        let cleaner = Cleaner::new(&cfg).unwrap();
        let result = cleaner.get_files_from_lakefs(20).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        //sort files by path
        let mut files = files
            .into_iter()
            .filter(|f| f.path.ends_with(".parquet"))
            .collect::<Vec<ObjectItem>>();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        //map to path
        let files = files.into_iter().map(|f| f.path).collect::<Vec<String>>();
        log::info!("files: {:?}", files);
        assert_eq!(files.len(), 20);
    }
}
