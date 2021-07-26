use std::fmt;
use std::fmt::Display;

pub mod coinmarket_scrapper;

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
