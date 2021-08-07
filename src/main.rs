pub mod assetmomentum;
pub mod config;
pub mod db;
pub mod frontend;
pub mod messari;

use config::*;
use crypto_scrapper::CoinMarketCapScrapper;
use frontend::*;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 1 {
        let scrapper = match CoinMarketCapScrapper::new(String::from("./config/config.toml")) {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "{}\nMake sure that you specify an existing configuration file.",
                    e.to_string()
                );
                return;
            }
        };
        cli_menu(scrapper);
    } else if args.len() == 2 {
        if args[1].eq("am") {
            // // assetmomentum::test(5);
            assetmomentum::init_asset_momentum(
                5,
                // String::from("2020-01-01"),
                String::from("2021-07-27"),
                String::from("2021-07-30"),
            );
        } else {
            // assetmomentum::test4();
            test5();
        }
    } else {
        println!("please use either no parameters or \"am\" to start assetmomentum function");
    }
}

pub fn test5() {
    let am_config = AssetMomentumConfig {
        watch_period_start_date: String::from("2020-01-01"),
        ranks_to_track: 500,
        db_name: String::from("asset_momentum"),
        db_uri: String::from("mongodb://localhost:27017"),
    };
    let rp = Replace {
        from: String::from("this is from"),
        to: String::from("this is from"),
    };
    let rp1 = Replace {
        from: String::from("this is from1"),
        to: String::from("this is from1"),
    };
    let config = Config {
        symbols: vec![String::from("symbol1"), String::from("symbol2")],
        regex_expressions: vec![String::from("symbol1"), String::from("symbol2")],
        replace_expressions: vec![String::from("symbol1"), String::from("symbol2")],
        about_regex: String::from("this is about_regex"),
        what_is_regex: String::from("this is what_is_regex"),
        title_regex: String::from("this is title_regex"),
        price_regex: String::from("this is price_regex"),
        price_percentage_regex: String::from("this is price_percentage_regex"),
        replace: vec![rp, rp1],
        asset_momentum_config: am_config,
    };
    let config_location = String::from("./config/config_inc_am.toml");

    let cobject = ConfigObject {
        configuration: config,
        source: config_location,
    };

    match cobject.store() {
        Ok(_) => println!("File Stored"),
        Err(e) => println!("Error occured while storing {}", e),
    }
}
