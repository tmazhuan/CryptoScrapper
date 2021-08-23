pub mod assetmomentum;
pub mod config;
pub mod db;
pub mod html;
pub mod messari;

use config::ConfigObject;
use html::ElementRelation::Child;
use html::{ElementRelation, HtmlParser, ParseError};
use regex::Regex;
use std::fmt;
use std::fmt::Display;
use std::io;
use std::sync::Arc;

///Structure of the result of a price query
pub struct PriceResult {
    symbol: String,
    price: f64,
    change: f64,
}

impl Display for PriceResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.symbol, self.price, self.change)
    }
}
impl PriceResult {
    pub fn to_string(&self) -> String {
        String::from(format!("{}\t{}", self.symbol, self.price))
    }
}
///Structure of the result of a Market query.
pub struct MarketResult {
    source: String,
    pair: String,
    price: f64,
    volume: f64,
    volume_percent: f64,
}

impl MarketResult {
    ///Returns the volume in USD
    pub fn get_volume_in_dollars(&self) -> f64 {
        if self.price == 0.0 || self.volume == 0.0 {
            0.0
        } else {
            self.volume / self.price
        }
    }
    ///Creates a string of length `i`
    fn get_spaces(i: usize) -> String {
        let mut temp = String::new();
        for _ in 0..i {
            temp.push_str(" ");
        }
        String::from(&temp)
    }

    ///Returns the header of the MarketResult table.
    pub fn get_header() -> String {
        let column_widht = 15;
        let source = String::from("Source");
        let mut result = String::from(&source);
        result.push_str(&MarketResult::get_spaces(column_widht - source.len()));
        let pair = String::from("Pair");
        result.push_str(&pair);
        result.push_str(&MarketResult::get_spaces(column_widht - pair.len()));
        let price = String::from("Price");
        result.push_str(&price);
        result.push_str(&MarketResult::get_spaces(column_widht - price.len()));
        let volume = String::from("Volume");
        result.push_str(&volume);
        result.push_str(&MarketResult::get_spaces(column_widht - volume.len()));
        let volume_percent = String::from("Volume %");
        result.push_str(&volume_percent);
        result.push_str(&MarketResult::get_spaces(
            column_widht - volume_percent.len(),
        ));
        return result;
    }
}
///Implements `Display` for Marketresult. The result is formated in a table retured as a string.
impl Display for MarketResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column_widht = 15;
        let mut result = String::from(&self.source);
        result.push_str(&MarketResult::get_spaces(column_widht - self.source.len()));
        result.push_str(&self.pair);
        result.push_str(&MarketResult::get_spaces(column_widht - self.pair.len()));
        let price = format!("{}", (self.price));
        result.push_str(&price);
        result.push_str(&MarketResult::get_spaces(column_widht - price.len()));
        let vol = format!("{}", (self.volume));
        result.push_str(&vol);
        result.push_str(&MarketResult::get_spaces(column_widht - vol.len()));
        let vol_per = format!("{}", (self.volume_percent));
        result.push_str(&vol_per);
        write!(f, "{}", result)
    }
}

///The structure to scrape Coinmarektcap.com. We store a `ConfigObject`, a `HtmlParser` and the `tokio Runtime`.
/// The structure and its methods encapsulate the asyn nature of the used code.
pub struct CoinMarketCapScrapper {
    pub cfg: ConfigObject,
    html_parser: HtmlParser,
    runtime: tokio::runtime::Runtime,
}

impl CoinMarketCapScrapper {
    ///Returns a new `CoinMarketCapScrapper` based on the `config_file_location`.
    /// # Arguments
    /// * `config_file_location` - A String that holds the location of the configuration file
    /// # Errors
    ///
    /// If the config is not available or another io error occurs an error is returned
    ///
    ///
    ///
    pub fn new(config_file_location: String) -> Result<CoinMarketCapScrapper, io::Error> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        Ok(CoinMarketCapScrapper {
            cfg: ConfigObject::new(&config_file_location)?,
            html_parser: rt.block_on(async { HtmlParser::new(45).await }).unwrap(),
            runtime: rt,
        })
    }

    ///Returns the detailed description of the currency `symbol`. The content of the result is taken from the "What is" section
    /// on CoinMarketcap.com. If "What is" is not available it returns the "Live Price Data section"
    /// # Arguments
    /// * `symbol`- a &str that holds the name of the symbol
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    ///
    pub fn get_details(&mut self, symbol: &str) -> Result<String, ParseError> {
        let url = format!("https://coinmarketcap.com/currencies/{}/", symbol);
        let c = Arc::clone(&self.html_parser.cache);
        let html = match self.runtime.block_on(html::get_html(c, &url, false, true)) {
            Ok(html) => html,
            Err(err) => return Err(err),
        };
        let mut what_is_inner = match html::get_inner_html_from_element(
            &self.cfg.configuration.what_is_regex,
            &html,
            vec![vec![ElementRelation::Parent]],
            symbol,
        ) {
            Ok(s) => String::from(&s[0]),
            Err(_) => {
                match html::get_inner_html_from_element(
                    &self.cfg.configuration.about_regex,
                    &html,
                    vec![vec![ElementRelation::Parent]],
                    symbol,
                ) {
                    Ok(s) => String::from(&s[0]),
                    Err(e) => return Err(e),
                }
            }
        };
        what_is_inner = self.cleanup_result_string(what_is_inner);
        Ok(what_is_inner)
    }

    ///Takes a string and removes all non-number characters from it and returns the cleaned string
    /// # Arguments
    /// * `input` - the string to cleanup as number
    fn cleanup_number(&self, input: &String) -> String {
        let regex = r#"[^\d.]"#;
        let regex = Regex::new(&regex).unwrap();
        String::from(regex.replace_all(&input, ""))
    }
    ///Takes a string and removes and replaces all elements stored in the config file.
    /// * Regex stored in the vector "regex_expressions" are removed
    /// * `Replace` structs stored in the vector "replace" are transformed from `Replace.from`to `Replace.to`
    /// * Strings stored in the Vector "replace_expression" are removed
    /// * Elements matching the regex expression stored in title_regex are formated as title
    fn cleanup_result_string(&self, mut text: String) -> String {
        for expr in &self.cfg.configuration.regex_expressions {
            let regex = Regex::new(&expr).unwrap();
            text = regex.replace_all(&text, "").into_owned();
        }
        for expr in &self.cfg.configuration.replace {
            let regex = Regex::new(&expr.from).unwrap();
            text = regex.replace_all(&text, expr.to.as_str()).into_owned();
        }
        text = text.replace("\\n", &format!("\n"));
        for expr in &self.cfg.configuration.replace_expressions {
            text = text.replace(expr, "");
        }
        let regex = Regex::new(&self.cfg.configuration.title_regex).unwrap();
        text = regex
            .replace_all(&text, "\n------------$1------------\n")
            .into_owned();
        String::from(text)
    }

    ///Returns all prices as a `PriceResult`Vector of the symbols stored in teh configuration file.
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    pub fn get_all_prices(&self) -> Result<Vec<PriceResult>, ParseError> {
        let s = &self.cfg.configuration.symbols;
        self.get_prices(s)
    }
    ///Returns all prices as a `PriceResult`Vector of the symbols stored in the `symbols` Vector passed to the function
    /// # Arguments
    /// * `symbols`- A vector containing the symbols for which the price should be returned
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    pub fn get_prices(&self, symbols: &Vec<String>) -> Result<Vec<PriceResult>, ParseError> {
        let price_regex = Arc::new(String::from(&self.cfg.configuration.price_regex));
        let price_per_regex =
            Arc::new(String::from(&self.cfg.configuration.price_percentage_regex));
        let x = self.runtime.block_on(async {
            let mut handle_vector = Vec::new();
            let mut result = Vec::new();
            {
                for s in symbols {
                    let cache = Arc::clone(&self.html_parser.cache);
                    let price_regex = Arc::clone(&price_regex);
                    let price_per_regex = Arc::clone(&price_per_regex);
                    let symbol = String::from(s);
                    let r = tokio::spawn(async move {
                        let url = format!("https://coinmarketcap.com/currencies/{}/", symbol);
                        let html = html::get_html(cache, &url, true, false).await.unwrap();
                        CoinMarketCapScrapper::parse_price(
                            html,
                            symbol,
                            price_regex,
                            price_per_regex,
                        )
                        .unwrap()
                    });
                    handle_vector.push(r);
                }
            }
            for h in handle_vector {
                result.push(h.await.unwrap());
            }
            result
        });
        Ok(x)
    }
    ///Parses the html snippet and extracts the price from it and creates a new PriceResult which is then returned.
    /// # Arguments
    /// * `html` - The html snippet which contains the price string.
    /// * `symbol` - The symbol for which the price is extracted.
    /// * `price_regex` - The regex expression to extract the price from the html snippet
    /// * `per_regex` - The regex expression to extract the percentage change from the html snippet
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    fn parse_price(
        html: String,
        symbol: String,
        reg_price_section: Arc<String>,
        per_regex: Arc<String>,
    ) -> Result<PriceResult, ParseError> {
        // println!("Parsing price for: {}", &symbol);
        let price = match html::get_inner_html_from_element(
            &reg_price_section,
            &html,
            vec![vec![ElementRelation::Child(0)]],
            &symbol,
        ) {
            Ok(s) => String::from(&s[0]),
            Err(e) => return Err(e),
        };
        // println!("Price received for: {}", &symbol);
        let price = price
            .replace("$", "")
            .replace(",", "")
            .parse::<f64>()
            .unwrap();
        //Percentage
        let percentage = match html::get_inner_html_from_element(
            &reg_price_section,
            &html,
            vec![vec![ElementRelation::Child(0), ElementRelation::Sibling(0)]],
            &symbol,
        ) {
            Ok(s) => String::from(&s[0]),
            Err(e) => return Err(e),
        };
        let re = Regex::new(&per_regex).unwrap();
        let cap = re.captures(&percentage);
        let change = match cap {
            Some(c) => {
                let p = String::from(&c[2]).parse::<f64>().unwrap();
                let s = match String::from(&c[1]).find("icon-Caret-up") {
                    Some(_) => 1.0,
                    None => -1.0,
                };
                p * s
            }
            None => {
                return Err(ParseError::new(format!(
                    "Error when reading page. Check manually for the symbol {}",
                    symbol
                )));
            }
        };
        Ok(PriceResult {
            symbol,
            price,
            change,
        })
    }
    ///Returns the price as a `PriceResult` of the symbols stored in the `symbol` passed to the function
    /// # Arguments
    /// * `symbol`- A string containing the symbols for which the price should be returned
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    pub fn get_price(&mut self, symbol: &str) -> Result<PriceResult, ParseError> {
        let mut result = self.get_prices(&vec![String::from(symbol)]).unwrap();
        Ok(result.pop().unwrap())
    }

    ///Returns Market data as a `MarketResult` of the symbol stored in the `symbol` passed to the function
    /// # Arguments
    /// * `symbol`- A string containing the symbols for which the price should be returned
    /// * `number_of_results`- Number of markets which has to be returned
    /// # Errors
    /// If there is a parse error or chromedriver is not available but needed an error is returned
    pub fn get_market_data(
        &mut self,
        symbol: &str,
        number_of_results: i32,
    ) -> Result<Vec<MarketResult>, ParseError> {
        let mut result = Vec::new();
        let url = format!("https://coinmarketcap.com/currencies/{}/markets", symbol);
        let c = Arc::clone(&self.html_parser.cache);
        let html = match self.runtime.block_on(html::get_html(c, &url, false, true)) {
            Ok(html) => html,
            Err(err) => return Err(err),
        };
        for i in 0..number_of_results {
            let regex = r#"<(table) (class=".*?currencies-markets_.*? ")>"#;
            let rel_source = vec![vec![
                Child(1),
                Child(i),
                Child(1),
                Child(0),
                Child(0),
                Child(1),
                Child(0),
            ]];
            let rel_pairs = vec![vec![Child(1), Child(i), Child(2), Child(0), Child(0)]];
            let rel_price = vec![vec![Child(1), Child(i), Child(3)]];
            let rel_vol = vec![vec![Child(1), Child(i), Child(4), Child(0)]];
            let rel_vol_perc = vec![vec![Child(1), Child(i), Child(5), Child(0), Child(0)]];
            let inner_source = html::get_inner_html_from_element(regex, &html, rel_source, symbol)?;
            let inner_pairs = html::get_inner_html_from_element(regex, &html, rel_pairs, symbol)?;
            let inner_price = html::get_inner_html_from_element(regex, &html, rel_price, symbol)?;
            let inner_vol = html::get_inner_html_from_element(regex, &html, rel_vol, symbol)?;
            let inner_vol_perc =
                html::get_inner_html_from_element(regex, &html, rel_vol_perc, symbol)?;
            result.push(MarketResult {
                source: String::from(&inner_source[0]),
                pair: String::from(&inner_pairs[0]),
                price: self.cleanup_number(&inner_price[0]).parse::<f64>().unwrap(),
                volume: self.cleanup_number(&inner_vol[0]).parse::<f64>().unwrap(),
                volume_percent: self
                    .cleanup_number(&inner_vol_perc[0].replace(",", "."))
                    .parse::<f64>()
                    .unwrap(),
            });
        } //for
        return Ok(result);
    } //fn get_market_data
}

#[cfg(test)]
mod tests {
    use super::CoinMarketCapScrapper;
    #[test]
    fn test_get_price_existing_symbol() {
        let mut scrapper =
            CoinMarketCapScrapper::new(String::from("./config/config.toml")).unwrap();
        let result = scrapper.get_price("multi-collateral-dai").unwrap();
        assert_eq!(result.price, 1.0); //testing with stable coin. expected value is 1.0
        assert!(result.change < 0.2 && result.change > -0.2); //testing with a stable coin. Difference should be less than 0.2%
    }
    #[test]
    fn test_get_price_non_existing_symbol() {
        let mut scrapper =
            CoinMarketCapScrapper::new(String::from("./config/config.toml")).unwrap();
        let result = scrapper.get_price("aseff");
        match result {
            Ok(_) => assert!(false, "expected an error"),
            Err(e) => assert!(true, format!("Error is {}", e)),
        };
    }
}
