use lakefs::LakeFsClient;

pub struct Cleaner {
    conn: duckdb::Connection,
    client: LakeFsClient
}

impl Cleaner {
    pub fn new() -> Self {
        let conn = duckdb::Connection::open("./data/lakefs.db").unwrap();
        Self { conn }
    }

}