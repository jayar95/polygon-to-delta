use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use backoff::future::retry;
use backoff::ExponentialBackoffBuilder;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::ensure_table_uri;
use deltalake::{DeltaOps, DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
use flate2::read::GzDecoder;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3CopyIfNotExists};
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, RetryConfig};
use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

static S3_RETRY_INITIAL_SLEEP_SECONDS: u64 = 33;
static S3_RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(S3_RETRY_INITIAL_SLEEP_SECONDS);
static S3_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(S3_RETRY_INITIAL_SLEEP_SECONDS * 4);
static S3_RETRY_MAX_ELAPSED_TIME: Option<Duration> = Some(Duration::from_secs(60 * 15));
static S3_RETRY_BACKOFF_INTERVAL_MULTIPLIER: f64 = 0.5;
static S3_RETRY_RANDOMIZATION_FACTOR: f64 = 0.5;

pub struct S3ConfigurationParams {
    pub endpoint: String,
    pub bucket_name: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

pub enum S3Provider {
    Polygon(S3ConfigurationParams),
    R2(S3ConfigurationParams),
}

impl S3Provider {
    pub fn new_polygon(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        S3Provider::Polygon(S3ConfigurationParams {
            endpoint: "https://files.polygon.io".to_string(),
            bucket_name: "flatfiles".to_string(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        })
    }

    pub fn new_r2(
        endpoint: impl Into<String>,
        bucket_name: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        S3Provider::R2(S3ConfigurationParams {
            endpoint: endpoint.into(),
            bucket_name: bucket_name.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        })
    }
}

pub struct S3ObjectStoreClientBuilder {
    builder: AmazonS3Builder,
}

impl S3ObjectStoreClientBuilder {
    pub fn new(config: S3Provider) -> Self {
        let r2_client_options = ClientOptions::default()
            .with_timeout_disabled()
            .with_connect_timeout_disabled();

        // https://docs.rs/object_store/0.9.0/src/object_store/aws/precondition.rs.html#30-31
        let r2_copy_if_not_exists_header = S3CopyIfNotExists::Header(
            "cf-copy-destination-if-none-match".to_string(),
            "*".to_string(),
        );

        let mut builder = AmazonS3Builder::new();

        match config {
            S3Provider::Polygon(params) => {
                builder = builder
                    .with_endpoint("https://files.polygon.io")
                    .with_bucket_name("flatfiles")
                    .with_access_key_id(params.access_key_id)
                    .with_retry(RetryConfig::default())
                    .with_secret_access_key(params.secret_access_key);
            }
            S3Provider::R2(params) => {
                builder = builder
                    .with_copy_if_not_exists(r2_copy_if_not_exists_header)
                    .with_client_options(r2_client_options)
                    .with_endpoint(params.endpoint)
                    .with_bucket_name(params.bucket_name)
                    .with_access_key_id(params.access_key_id)
                    .with_retry(RetryConfig::default())
                    .with_secret_access_key(params.secret_access_key)
                    .with_disable_tagging(true);
            }
        }

        Self { builder }
    }

    pub fn build(&self) -> impl DeltaIO {
        self.builder.clone().build().unwrap()
    }

    pub fn build_with_url(&self, url: impl Into<String>) -> impl DeltaIO {
        self.builder.clone().with_url(url).build().unwrap()
    }
}

#[async_trait]
pub trait DeltaIO: ObjectStore {
    #[instrument(skip_all)]
    async fn get_decompressed_file_cursor(
        &self,
        s3_path: String,
    ) -> Result<Box<Cursor<Vec<u8>>>, anyhow::Error> {
        let path = Path::from(s3_path);

        let backoff_strategy = ExponentialBackoffBuilder::new()
            .with_initial_interval(S3_RETRY_INITIAL_INTERVAL)
            .with_max_interval(S3_RETRY_MAX_INTERVAL)
            .with_max_elapsed_time(S3_RETRY_MAX_ELAPSED_TIME)
            .with_multiplier(S3_RETRY_BACKOFF_INTERVAL_MULTIPLIER)
            .with_randomization_factor(S3_RETRY_RANDOMIZATION_FACTOR)
            .build();

        let compressed_bytes = retry(backoff_strategy, || async {
            Ok(self.get(&path).await?.bytes().await?)
        })
        .await?;

        let mut data = Vec::new();
        GzDecoder::new(compressed_bytes.as_ref()).read_to_end(&mut data)?;

        Ok(Box::new(Cursor::new(data)))
    }

    #[instrument(skip_all)]
    async fn write_to_delta(
        self,
        data: Box<Cursor<Vec<u8>>>,
        uri: String,
        schema: Schema,
        format: Format,
        save_mode: SaveMode,
    ) -> DeltaResult<DeltaTable>
    where
        Self: Sized,
    {
        const BATCH_SIZE: usize = 10_000;

        let csv_reader = ReaderBuilder::new(SchemaRef::from(schema))
            .with_format(format)
            .with_batch_size(BATCH_SIZE)
            .build(data)?;

        let table_uri = ensure_table_uri(uri.clone()).unwrap();

        let mut table = DeltaTableBuilder::from_uri(uri)
            .with_storage_backend(Arc::new(self), table_uri)
            .build()?;

        let ops: DeltaOps = match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
        .expect("Failed to load table");

        ops.write(csv_reader.map(|c| c.unwrap()))
            .with_save_mode(save_mode)
            .await
    }
}

impl DeltaIO for AmazonS3 {}
