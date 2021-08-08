use super::db::AssetMommentumMongo;
use super::messari;
use crate::config::ConfigObject;
use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::io;

const ONE_DAY_IN_SECONDS: i64 = 86400;

pub struct AssetMomentum {
    pub config: ConfigObject,
    db: AssetMommentumMongo,
}
#[derive(Serialize, Deserialize)]
pub struct Asset {
    pub slug: String,
    pub last_update_sec: i64,
}

pub struct AssetMetrics {
    pub slug: String,
    pub last_update_sec: i64,
    pub data: Vec<MetricsTimeSeriesElement>,
}

#[derive(Serialize, Deserialize)]
pub struct MetricsTimeSeriesElement {
    pub timestamp_sec: i64,
    pub value: f64,
    pub metrics_id: String,
}
impl AssetMomentum {
    ///Returns a new `AssetMomentum` based on the `config_file_location`.
    /// # Arguments
    /// * `config_file_location` - A String that holds the location of the configuration file
    /// # Errors
    ///
    /// If the config is not available or another io error occurs an error is returned
    ///
    ///
    ///
    pub fn new(config_file_location: String) -> Result<AssetMomentum, io::Error> {
        let c = ConfigObject::new(&config_file_location)?;
        Ok(AssetMomentum {
            config: ConfigObject::new(&config_file_location)?,
            db: AssetMommentumMongo::new(
                &c.configuration.asset_momentum_config.db_name,
                &c.configuration.asset_momentum_config.db_uri,
                &c.configuration.asset_momentum_config.symbol_collection,
                &c.configuration.asset_momentum_config.metrics_collection,
                &c.configuration.asset_momentum_config.ignore_symbols_name,
            ),
        })
    }

    pub fn get_performance(from: String, to: String) {
        let from = NaiveDate::parse_from_str(&from, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        let to = NaiveDate::parse_from_str(&to, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
    }

    pub fn init_asset_momentum(&self, to: String) {
        //drop Symbols collection if existing
        self.db.drop_metric_collections();
        self.db.drop_symbol_collection();
        //get symbol/slug combination
        let messari_assets = messari::get_symbols(
            self.config
                .configuration
                .asset_momentum_config
                .ranks_to_track,
        );
        //lets remove the assets we want to ignore
        let messari_assets_filterd: Vec<Asset> = messari_assets
            .into_iter()
            .filter(|asset| self.db.get_symbol_to_ignore(&asset.slug).is_none())
            .collect();
        self.db.store_symbols(&messari_assets_filterd);
        //for each symbol load the price data
        self.get_metrics_data(
            messari_assets_filterd,
            // &from,
            &to,
            &String::from("price"),
            None,
            Some(String::from("columns=close")),
        );
    }

    pub fn update_asset_momentum(&self, to: &String) {
        //get current top x assets
        let messari_assets = messari::get_symbols(
            self.config
                .configuration
                .asset_momentum_config
                .ranks_to_track,
        );
        //filter out the symbols to ignore
        //lets remove the assets we want to ignore
        let messari_assets_filtered: Vec<Asset> = messari_assets
            .into_iter()
            .filter(|asset| self.db.get_symbol_to_ignore(&asset.slug).is_none())
            .collect();
        //create two groups with the remaining assets
        let db_assets = self.db.get_slugs_with_last_update_date();
        let mut assets_to_update: Vec<Asset> = Vec::new();
        for ma in messari_assets_filtered {
            let da = db_assets.get(&ma.slug);
            match da {
                Some(a) => assets_to_update.push(Asset {
                    slug: String::from(&a.slug),
                    last_update_sec: a.last_update_sec,
                }),
                None => {
                    //store asset in symbols collection
                    self.db.store_symbol(&ma);
                    assets_to_update.push(ma);
                }
            }
        }
        self.get_metrics_data(
            assets_to_update,
            // &from,
            &to,
            &String::from("price"),
            None,
            Some(String::from("columns=close")),
        );
    }

    fn get_metrics_data(
        &self,
        messari_assets_filterd: Vec<Asset>,
        // from: &String,
        to_date: &String,
        metric_id: &String,
        _alternate_metric: Option<String>,
        parameters: Option<String>,
    ) {
        let to_date_in_seconds = NaiveDate::parse_from_str(to_date, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        for asset in messari_assets_filterd {
            let from = if asset.last_update_sec <= 0 {
                String::from(
                    &self
                        .config
                        .configuration
                        .asset_momentum_config
                        .observation_period_start_date,
                )
            } else {
                format!(
                    "{}",
                    Utc.timestamp(asset.last_update_sec + ONE_DAY_IN_SECONDS, 0)
                        .format("%Y-%m-%d")
                )
            };
            if asset.last_update_sec + ONE_DAY_IN_SECONDS > to_date_in_seconds {
                println!("From Date {} is bigger than to Date {}", from, to_date);
                continue;
            }
            let metric_data = messari::get_1_item_metric_history(
                &asset.slug,
                &metric_id,
                None,
                &from,
                &to_date,
                &parameters,
            );
            if metric_data.is_none() {
                //add the slug to our ignore collection
                self.db.store_symbol_to_ignore(&asset.slug);
                //delete the slug from our symbols database
                self.db.delete_symbol(&asset.slug);
                //go to the next symbol
                continue;
            }
            let metric = metric_data.unwrap();
            let slug = String::from(&metric.slug);
            let lastupdate = metric.last_update_sec;
            self.db.store_metric(metric);
            self.db.update_lastupdate_of_symbol(slug, lastupdate);
        }
    }
}
impl MetricsTimeSeriesElement {
    pub fn vec_from(
        data: Vec<(i64, f64)>,
        metrics_id: String,
    ) -> (Vec<MetricsTimeSeriesElement>, i64) {
        let mut result: Vec<MetricsTimeSeriesElement> = Vec::with_capacity(data.len());
        let mut maxtimestamp = 0;
        for (t, v) in data {
            result.push(MetricsTimeSeriesElement {
                timestamp_sec: t / 1000,
                value: v,
                metrics_id: String::from(&metrics_id),
            });
            if (t / 1000) > maxtimestamp {
                maxtimestamp = t / 1000;
            }
        }
        return (result, maxtimestamp);
    }
}
