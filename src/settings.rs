use config::{Config, ConfigError, Environment};
use serde::{Deserialize};
use std::env;
use dotenv::dotenv;

#[derive(Debug, Deserialize)]
pub struct Settings {
	pub polygon_key_id: String,
	pub polygon_access_key: String,
	pub delta_s3_endpoint: String,
	pub delta_s3_bucket_name: String,
	pub delta_s3_key_id: String,
	pub delta_s3_access_key: String,
	pub delta_save_mode: String,
	pub start_year: u32,
	pub end_year: u32,
	pub start_month: u32,
	pub end_month: u32,
	pub skip_existing: bool,
}

impl Settings {
	pub fn new() -> Result<Self, ConfigError> {
		let is_dev = env::var("RUST_ENV")
			.map_or(true, |v| v == "development");

		if is_dev {
			dotenv().ok();
		}

		let config = Config::builder()
			.add_source(Environment::default())
			.set_default("DELTA_SAVE_MODE", "append")?
			.set_default("START_YEAR", 2022)?
			.set_default("END_YEAR", 2023)?
			.set_default("START_MONTH", 11)?
			.set_default("END_MONTH", 12)?
			.set_default("SKIP_EXISTING", false)?
			.build()?;

		config.try_deserialize()
	}
}

