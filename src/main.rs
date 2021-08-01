pub mod assetmomentum;
pub mod config;
pub mod frontend;

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
            // assetmomentum::test(40);
            assetmomentum::init_asset_momentum(
                700,
                String::from("2020-01-01"),
                // String::from("2021-07-27"),
                String::from("2021-07-30"),
            );
        } else {
            assetmomentum::test2();
            println!("please use either no parameters or \"am\" to start assetmomentum function");
        }
    } else {
        println!("please use either no parameters or \"am\" to start assetmomentum function");
    }
}
