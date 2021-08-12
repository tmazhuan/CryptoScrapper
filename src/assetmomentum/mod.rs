use super::db::AssetMommentumMongo;
use super::messari;
use crate::config::ConfigObject;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::io;

const ONE_DAY_IN_SECONDS: i64 = 86400;

pub struct AssetMomentum {
    pub config: ConfigObject,
    db: AssetMommentumMongo,
}
#[derive(Serialize, Deserialize)]
pub struct Asset {
    pub slug: String,
    pub first_entry_sec: i64,
    pub last_update_sec: i64,
}

pub struct AssetMetrics {
    pub slug: String,
    pub last_update_sec: i64,
    pub first_item_sec: i64,
    pub data: Vec<MetricsTimeSeriesElement>,
}
pub struct AssetPerformanceResult {
    pub slug: String,
    pub start_value: MetricsTimeSeriesElement,
    pub end_value: MetricsTimeSeriesElement,
    pub abs_change: f64,
    pub percentage_change: f64,
}
#[derive(Serialize, Deserialize)]
pub struct MetricsTimeSeriesElement {
    pub timestamp_sec: i64,
    pub value: f64,
    pub metrics_id: String,
}
impl Display for AssetPerformanceResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t\t{}\t{}\t{}\t{}\t{}\t\t\t{}",
            self.slug,
            NaiveDateTime::from_timestamp(self.start_value.timestamp_sec, 0).format("%F"),
            NaiveDateTime::from_timestamp(self.end_value.timestamp_sec, 0).format("%F"),
            format!("{:.1$}", self.start_value.value, 2),
            format!("{:.1$}", self.end_value.value, 2),
            format!("{:.1$}", self.abs_change, 2),
            format!("{:.1$}", self.percentage_change * 100.0, 2),
        )
    }
}
impl AssetPerformanceResult {
    pub fn table_header() -> String {
        format!(
            "{}\t\t{}\t\t{}\t\t{}\t{}\t{}\t\t{}",
            "Slug", "From", "to", "value USD", "value USD", "change USD", "change %",
        )
    }
    pub fn from(
        slug: &String,
        start: &MetricsTimeSeriesElement,
        end: &MetricsTimeSeriesElement,
    ) -> AssetPerformanceResult {
        return AssetPerformanceResult {
            slug: String::from(slug),
            abs_change: &end.value - &start.value,
            percentage_change: (&end.value - &start.value) / &start.value,
            start_value: MetricsTimeSeriesElement {
                timestamp_sec: start.timestamp_sec,
                value: start.value,
                metrics_id: String::from(&start.metrics_id),
            },
            end_value: MetricsTimeSeriesElement {
                timestamp_sec: end.timestamp_sec,
                value: end.value,
                metrics_id: String::from(&end.metrics_id),
            },
        };
    }
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
    pub fn get_performance_of_asset(
        &self,
        slug: String,
        from: String,
        to: String,
    ) -> Option<AssetPerformanceResult> {
        let mut from = NaiveDate::parse_from_str(&from, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        let mut to = NaiveDate::parse_from_str(&to, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        let asset = self.db.get_slug_summary(&slug);
        if asset.is_none() {
            println!("Asset {} not found.", slug);
            return None;
        }
        let asset = asset.unwrap();
        if asset.first_entry_sec > from {
            from = asset.first_entry_sec;
        }
        if asset.last_update_sec < to {
            to = asset.last_update_sec;
        }
        let start = self
            .db
            .get_metric(&asset, &String::from("price"), from, None);
        let end = self.db.get_metric(&asset, &String::from("price"), to, None);
        if start.is_none() || end.is_none() {
            return None;
        };

        return Some(AssetPerformanceResult::from(
            &asset.slug,
            &start.unwrap().data[0],
            &end.unwrap().data[0],
        ));
    }
    pub fn get_daily_performance_of_asset(
        &self,
        slug: String,
        from: &String,
        to: &String,
    ) -> Option<Vec<AssetPerformanceResult>> {
        let mut from = NaiveDate::parse_from_str(&from, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        let mut to = NaiveDate::parse_from_str(&to, "%Y-%m-%d")
            .unwrap()
            .and_hms_milli(0, 0, 0, 0)
            .timestamp();
        let asset = self.db.get_slug_summary(&slug);
        if asset.is_none() {
            println!("Asset {} not found.", slug);
            return None;
        }
        let asset = asset.unwrap();
        if asset.first_entry_sec > from {
            from = asset.first_entry_sec;
        }
        if asset.last_update_sec < to {
            to = asset.last_update_sec;
        }

        let metrics = self
            .db
            .get_metric(&asset, &String::from("price"), from, Some(to));
        if metrics.is_none() {
            println!("there was a mistake while getting metrics");
            return None;
        }
        let metrics = metrics.unwrap();
        let mut performance: Vec<AssetPerformanceResult> = Vec::new();
        for i in 0..metrics.data.len() - 1 {
            performance.push(AssetPerformanceResult::from(
                &asset.slug,
                &metrics.data[i],
                &metrics.data[i + 1],
            ));
        }
        return Some(performance);
    }
    pub fn get_daily_performance_all_assets(
        &self,
        from: String,
        to: String,
    ) -> HashMap<String, Option<Vec<AssetPerformanceResult>>> {
        let assets = self.db.get_slug_summaries();
        let mut result: HashMap<String, Option<Vec<AssetPerformanceResult>>> = HashMap::new();
        for (slug, asset) in assets {
            result.insert(
                slug,
                self.get_daily_performance_of_asset(asset.slug, &from, &to),
            );
        }
        return result;
    }

    pub fn init_asset_momentum(&self, to: String) {
        println!("Starting AM init...");
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
        let db_assets = self.db.get_slug_summaries();
        let mut assets_to_update: Vec<Asset> = Vec::new();
        for ma in messari_assets_filtered {
            let da = db_assets.get(&ma.slug);
            match da {
                Some(a) => assets_to_update.push(Asset {
                    slug: String::from(&a.slug),
                    first_entry_sec: a.first_entry_sec,
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
            let first_entry = metric.first_item_sec;
            self.db.store_metric(metric);
            self.db.update_slug_summary(slug, first_entry, lastupdate);
        }
    }
}
impl MetricsTimeSeriesElement {
    pub fn vec_from(
        data: Vec<(i64, f64)>,
        metrics_id: String,
    ) -> (Vec<MetricsTimeSeriesElement>, i64, i64) {
        let mut result: Vec<MetricsTimeSeriesElement> = Vec::with_capacity(data.len());
        let mut maxtimestamp = 0;
        let mut mintimestamp = i64::MAX;
        for (t, v) in data {
            let t_sec = t / 1000;
            result.push(MetricsTimeSeriesElement {
                timestamp_sec: t_sec,
                value: v,
                metrics_id: String::from(&metrics_id),
            });
            if t_sec > maxtimestamp {
                maxtimestamp = t_sec;
            }
            if t_sec < mintimestamp {
                mintimestamp = t_sec
            }
        }
        return (result, mintimestamp, maxtimestamp);
    }
}
