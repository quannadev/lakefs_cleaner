pub fn setup_s3(s3_endpoint: String, s3_access_key: String, s3_secret_key: String) -> String {
    format!(
        r#"
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='{s3_endpoint}';
SET s3_region='us-east-1';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='{s3_access_key}';
SET s3_secret_access_key='{s3_secret_key}';
"#,
    )
}
