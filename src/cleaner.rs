use lakefs::{Config, LakeFsClient};

pub struct Cleaner {
    conn: duckdb::Connection,
    client: LakeFsClient
}

impl Cleaner {
    pub fn new(cfg: Config) -> Self {
        let conn = duckdb::Connection::open("./data/lakefs.db").unwrap();
        let client = LakeFsClient::new(cfg);
        Self { conn, client }
    }



}