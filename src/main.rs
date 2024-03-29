use tokio::sync::Semaphore;
use tokio::task;
use futures_util::StreamExt;
use anyhow::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::io::{Cursor};
use deltalake::aws::register_handlers;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use object_store::ObjectStore;
use object_store::path::Path;
use arrow::csv::reader::Format;
use arrow::datatypes::Schema;
use tracing::{info, instrument, error, Level, debug};
use tracing_subscriber::FmtSubscriber;

mod settings;
mod delta;
use crate::delta::{DeltaIO, S3ObjectStoreClientBuilder, S3Provider};
use crate::settings::Settings;

type ArrowMeta = (Format, Schema);

#[tokio::main]
#[instrument]
async fn main() -> Result<(), anyhow::Error> {
	let subscriber = FmtSubscriber::builder()
		.with_max_level(Level::INFO)
		.finish();

	tracing::subscriber::set_global_default(subscriber)
		.expect("setting default subscriber failed");

	register_handlers(None);

	let settings = Settings::new().expect("Loading settings failed");

	let delta_save_mode = SaveMode::from_str(&settings.delta_save_mode)?;
	let start_year = settings.start_year;
	let end_year = settings.end_year;
	let start_month = settings.start_month;
	let end_month = settings.end_month;

	let polygon_s3 = S3ObjectStoreClientBuilder::new(S3Provider::new_polygon(
			settings.polygon_key_id,
			settings.polygon_access_key,
	));

	let delta_s3_uri = format!("s3a://{}", settings.delta_s3_bucket_name);

	let delta_s3 = S3ObjectStoreClientBuilder::new(S3Provider::new_r2(
		settings.delta_s3_endpoint,
		settings.delta_s3_bucket_name,
		settings.delta_s3_key_id,
		settings.delta_s3_access_key,
	));

	let initial_data_sample = polygon_s3.build()
		.get_decompressed_file_cursor(
			"us_stocks_sip/minute_aggs_v1/2023/12/2023-12-29.csv.gz".to_string(),
		).await?;

	let (
		format,
		schema
	) = get_arrow_meta_from_cursor(*initial_data_sample)?;

	let semaphore = Arc::new(Semaphore::new(20));
	let mut handles = vec![];

	for year in start_year..=end_year {
		for month in start_month..=end_month {
			let month_dir = if month < 10 {
				format!("0{}", month)
			} else {
				format!("{}", month)
			};

			let path = Path::from(
				format!("us_stocks_sip/minute_aggs_v1/{year}/{month_dir}/")
			);

			let client = polygon_s3.build();

			let mut file_list = client.list(Some(&path));

			while let Some(meta) = file_list.next().await.transpose()? {
				let day = meta.location.filename().unwrap()
					.split("-")
					.collect::<Vec<&str>>()
					.get(2).unwrap()
					.get(0..2).unwrap();

				let delta_path = format!("delta/{year}/{month}/{day}");
				let uri = format!("{delta_s3_uri}/{delta_path}");

				if settings.skip_existing {
					debug!("SKIP_EXISTING set to true, checking {}", uri);

					let current = delta_s3.build()
						.list(Some(&Path::from(delta_path)))
						.count().await;

					if current > 0 {
						info!("Existing table found at {} - Skipping...", uri);
						continue
					}
				}

				let table = decompress_and_write_from_s3_to_delta(
					polygon_s3.build(),
					meta.location.to_string(),
					delta_s3.build_with_url(uri.clone()),
					uri.clone(),
					schema.clone(),
					format.clone(),
					delta_save_mode.clone()
				);

				let permit = semaphore.clone().acquire_owned().await.unwrap();

				let handle = task::spawn(async move {
					match table.await {
						Ok(_) => {
							drop(permit);
						}
						Err(_) => {
							error!("Failed to write: {uri}");
							drop(permit);
						}
					}
				});

				handles.push(handle);
			}
		}
	}

	for handle in handles {
		handle.await?;
	}

	Ok(())
}

#[instrument(skip_all)]
async fn decompress_and_write_from_s3_to_delta(
	s3: impl DeltaIO,
	s3_path: String,
	delta_s3: impl DeltaIO,
	delta_uri: String,
	schema: Schema,
	format: Format,
	save_mode: SaveMode
) -> Result<DeltaTable, Error> {
	info!("Reading From: {}", s3_path);

	let data = s3.get_decompressed_file_cursor(s3_path).await?;

	info!("Saving to: {}", delta_uri);

	let table = delta_s3.write_to_delta(
		data,
		delta_uri,
		schema,
		format,
		save_mode,
	).await.expect("Failed to write to table");

	println!("Completed Saving Table at URI: {}", table.table_uri());

	Ok(table)
}

#[instrument(skip_all)]
fn get_arrow_meta_from_cursor(
	cursor: Cursor<Vec<u8>>,
) -> Result<ArrowMeta, anyhow::Error> {
	let format = Format::default().with_header(true);
	let (schema, _) = format.infer_schema(
		cursor,
		Some(10),
	)?;

	Ok((format, schema))
}
