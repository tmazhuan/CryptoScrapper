use crate::config::{StakingConfigObject, StakingSetup};
use crate::html;
use crate::html::{HtmlParser, ParseError};
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct StakingResult {
    symbol: String,
    date: i128,
    amount_original: f64,
    dollar_rate_at_date: f64,
    amount_dollar: f64,
}

pub struct StakingScrapper {
    pub cfg: StakingConfigObject,
    html_parser: HtmlParser,
    runtime: tokio::runtime::Runtime,
}

impl StakingScrapper {
    ///Returns a new `stakingScrapper` based on the `config_file_location`.
    /// # Arguments
    /// * `config_file_location` - A String that holds the location of the configuration file
    /// # Errors
    ///
    /// If the config is not available or another io error occurs an error is returned
    pub fn new(config_file_location: String) -> Result<StakingScrapper, io::Error> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        Ok(StakingScrapper {
            cfg: StakingConfigObject::new(config_file_location)?,
            html_parser: rt.block_on(async { HtmlParser::new(45).await }).unwrap(),
            runtime: rt,
        })
    }

    pub fn add_staking_setup(&mut self, setup: StakingSetup) {
        self.cfg.configuration.staking_setups.push(setup);
    }

    pub fn get_unclaimed_staking_rewards(&self, url: String) -> Result<String, ParseError> {
        let url = format!("{}", url);
        let c = Arc::clone(&self.html_parser.cache);
        let html = match self.runtime.block_on(html::get_html(c, &url, false, true)) {
            Ok(html) => html,
            Err(err) => return Err(err),
        };
        return Ok(html);
    }
}
