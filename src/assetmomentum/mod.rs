use core::time::Duration;
use mongodb::options::{DropCollectionOptions, WriteConcern};
use mongodb::{bson, bson::doc, sync::Client};
use reqwest::header;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::env;
use std::{thread, time};

const MAX_PAGE_SIZE: i32 = 500;
const DB_NAME: &str = "asset_momentum";
const DB_URI: &str = "mongodb://localhost:27017";
const SYMBOLS_COLLECTION_NAME: &str = "symbols";
const METRICS_COLLECTION_NAME: &str = "metrics";
const SYMBOLS_IGNORE_COLLECTION_NAME: &str = "ignore_symbols";

#[derive(Serialize, Deserialize)]
pub struct AssetResult {
    // #[serde(skip_deserializing)]
    // slug: String,
    // status: Status,//we are not interested in the status
    #[serde(rename(deserialize = "data"))]
    symbols: Vec<Symbol>,
}
#[derive(Serialize, Deserialize)]
pub struct Status {
    elapsed: i32,
    timestamp: String,
}
#[derive(Serialize, Deserialize)]
pub struct Symbol {
    slug: String,
    // #[serde(default = "default_symbol")]
    #[serde(deserialize_with = "parse_symbol")]
    symbol: String,
}

fn parse_symbol<'de, D>(d: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Deserialize::deserialize(d).map(|x: Option<_>| x.unwrap_or("".to_string()))
}

#[derive(Serialize, Deserialize)]
pub struct MessariAssetMetrics {
    // status: Status,
    data: MetricsData,
}
#[derive(Serialize, Deserialize)]
pub struct MetricsData {
    // parameters: MetricsParameter,//we are not interested in the parameters
    // schema: MetricsSchema,//we are not interessted in the MetricsSchema
    schema: MetricsSchema,
    values: Vec<(i64, f64)>,
}
#[derive(Serialize, Deserialize)]
pub struct MetricsParameter {
    asset_id: String,
    asset_key: String,
    columns: Vec<String>,
    end: String,
    format: String,
    interval: String,
    start: String,
    timestamp_format: String,
}
#[derive(Serialize, Deserialize)]
pub struct MetricsSchema {
    description: String,
    metric_id: String,
    // minimum_interval: String,
    // source_attribution: Vec<SourceAttribution>,
    // values_schema: Value,
}
#[derive(Serialize, Deserialize)]
pub struct SourceAttribution {
    name: String,
    url: String,
}
#[derive(Serialize, Deserialize)]
pub struct AssetMetrics {
    slug: String,
    timeseries: HashMap<String, MetricsTimeSeries>,
    mcaptype: String,
}
#[derive(Serialize, Deserialize)]
pub struct MetricsTimeSeries {
    timestamp: i64,
    mcap: f64,
    price: f64,
}
pub fn test() {
    let s = Symbol {
        slug: String::from("bitcoin"),
        symbol: String::from("btc"),
    };
    store_symbol_to_ignore(&s);
}
pub fn test2() {
    let x = get_symbol_to_ignore(&String::from("bitcoin"));
    match x {
        Some(symbol) => println!("{}-{}", symbol.slug, symbol.symbol),
        None => println!("Symbol not found"),
    }
}

pub fn update asset_momentum(){
    //get top x assets
    //filter out the symbols to ignore
    //create two groups with the remaining assets
    //1st group are completely new assets not yet in the metrics DB
    //get the data for these assets from 1.1.2020 to yesterday
    //2nd group are already existing assets
    //get the data from the last in the db existing day to yesterday. this could be different for each symbols
}
pub fn init_asset_momentum(ranks_to_track: i32, from: String, to: String) {
    //drop database if existing
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    //drop symbols
    let mut result = database.collection(SYMBOLS_COLLECTION_NAME).drop(
        DropCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(None)
                    .w_timeout(Some(Duration::new(5, 0)))
                    .journal(Some(false))
                    .build(),
            )
            .build(),
    );
    match result {
        Ok(_) => println!("Drop of collection {} successful!", SYMBOLS_COLLECTION_NAME),
        Err(e) => {
            println!(
                "Drop of collection {} not successful!\n{:?}",
                SYMBOLS_COLLECTION_NAME, e
            );
            return;
        }
    }
    result = database.collection(METRICS_COLLECTION_NAME).drop(
        DropCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(None)
                    .w_timeout(Some(Duration::new(5, 0)))
                    .journal(Some(false))
                    .build(),
            )
            .build(),
    );
    match result {
        Ok(_) => println!("Drop of collection {} successful!", METRICS_COLLECTION_NAME),
        Err(e) => {
            println!(
                "Drop of collection {} not successful!\n{:?}",
                METRICS_COLLECTION_NAME, e
            );
            return;
        }
    }
    //get symbol/slug combination
    let mut assets = get_symbols(ranks_to_track);
    //lets remove the assets we want to ignore
    assets.symbols = assets
        .symbols
        .into_iter()
        .filter(|asset| get_symbol_to_ignore(&asset.slug).is_none())
        .collect();
    store_symbols(&assets);
    // let mut metric_items = HashMap::new();
    //for each symbol load the price data
    //let mut to_store = Vec::new();
    for asset in assets.symbols {
        let (price, _) = get_1_item_metric_history(
            String::from(&asset.slug),
            String::from("price"),
            None,
            from.clone(),
            to.clone(),
            Some(String::from("columns=close")),
        );
        let (mcap, mcaptype) = get_1_item_metric_history(
            String::from(&asset.slug),
            String::from("mcap.out"),
            Some(String::from("mcap.circ")),
            from.clone(),
            to.clone(),
            None,
        );
        // check if we get a SOME back from our api. if one of the metrics is NONE we ignore the slug
        // to ensure, that we remember the slugs to ignore we write the to a new collection
        // and delete the slug from the symbols database
        if price.is_none() || mcap.is_none() {
            //add the slug to our ignore collection
            store_symbol_to_ignore(&asset);
            //delete the slug from our symbols database
            delete_symbol(&asset);
            //go to the next symbol
            continue;
        }
        store_metric(combine_metrics(
            String::from(&asset.slug),
            to_hash_map(price.unwrap().data.values),
            to_hash_map(mcap.unwrap().data.values),
            mcaptype,
        ));
        // to_store.push(combine_metrics(
        //     String::from(&asset.slug),
        //     to_hash_map(price.unwrap().data.values),
        //     to_hash_map(mcap.unwrap().data.values),
        //     mcaptype,
        // ));
    }
    //store_metric(to_store);
}

fn to_hash_map(input: Vec<(i64, f64)>) -> HashMap<String, f64> {
    let mut result = HashMap::new();
    for (timestamp, value) in input {
        result.insert(timestamp.to_string(), value);
    }
    return result;
}
fn combine_metrics(
    slug: String,
    price: HashMap<String, f64>,
    mcap: HashMap<String, f64>,
    mcaptype: String,
) -> AssetMetrics {
    let mut result = AssetMetrics {
        slug: slug,
        timeseries: HashMap::new(),
        mcaptype: mcaptype,
    };
    for (timestamp, price_value) in price {
        result.timeseries.insert(
            String::from(&timestamp),
            MetricsTimeSeries {
                timestamp: String::from(&timestamp).parse::<i64>().unwrap(),
                price: price_value,
                mcap: *mcap.get(&timestamp).unwrap_or(&0.0f64),
            },
        );
    }
    return result;
}

fn get_symbols(ranks_to_track: i32) -> AssetResult {
    //Maximum number of result from API is 500
    //Lets see if we want 500 or more results
    let pages;
    let page_size;
    if ranks_to_track <= MAX_PAGE_SIZE {
        pages = 1;
        page_size = ranks_to_track;
    } else if ranks_to_track > MAX_PAGE_SIZE && ranks_to_track < 1000 {
        pages = ranks_to_track / MAX_PAGE_SIZE + 1;
        page_size = MAX_PAGE_SIZE;
    } else {
        pages = 0;
        page_size = 0;
    }
    let mut result: AssetResult = AssetResult {
        symbols: Vec::with_capacity((pages * page_size) as usize),
    };
    let mut headers = header::HeaderMap::new();
    let api_key = env::var("API_KEY").unwrap();
    headers.insert(
        "x-messari-api-key",
        header::HeaderValue::from_str(&api_key).unwrap(),
    );
    let client = reqwest::blocking::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    // let client = reqwest::blocking::Client::new();
    for p in 0..pages {
        let url = format!(
            "http://data.messari.io/api/v2/assets?fields=slug,symbol&limit={}&page={}",
            page_size,
            p + 1
        );
        println!("Calling {}", url);
        // let resp = reqwest::blocking::get(url).unwrap().text().unwrap();
        // get a client builder
        let resp = client.get(url).send().unwrap().text().unwrap();
        let mut v: AssetResult = serde_json::from_str(&resp).unwrap();
        result.symbols.append(&mut v.symbols);
    }
    // result.symbols.truncate((pages*page_size-(page_size-remainder)) as usize);
    result.symbols.truncate(ranks_to_track as usize);
    println!("Length of result: {}", result.symbols.len());
    return result;
}
pub fn store_symbols(data: &AssetResult) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    let mut docs = Vec::new();
    for symbol in &data.symbols {
        docs.push(bson::to_document(&symbol).unwrap());
    }
    collection.insert_many(docs, None).unwrap();
}
fn store_symbol_to_ignore(data: &Symbol) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_IGNORE_COLLECTION_NAME);
    collection
        .insert_one(bson::to_document(&data).unwrap(), None)
        .unwrap();
}
fn delete_symbol_to_ignore(data: &Symbol) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_IGNORE_COLLECTION_NAME);
    collection
        .delete_one(bson::to_document(&data).unwrap(), None)
        .unwrap();
}
fn get_symbol_to_ignore(slug: &String) -> Option<Symbol> {
    let mut doc = bson::Document::new();
    doc.insert(String::from("slug"), slug);
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_IGNORE_COLLECTION_NAME);
    let r = collection.find_one(doc, None);
    return match r {
        Ok(result) => match result {
            Some(x) => bson::from_document(x).unwrap(),
            None => None,
        },
        Err(_) => None,
    };
}

fn delete_symbol(data: &Symbol) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    collection
        .delete_one(bson::to_document(&data).unwrap(), None)
        .unwrap();
}

fn store_metrics(data: Vec<AssetMetrics>) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(METRICS_COLLECTION_NAME);
    let mut docs = Vec::new();
    for metric in &data {
        docs.push(bson::to_document(&metric).unwrap());
    }
    collection.insert_many(docs, None).unwrap();
}
fn store_metric(data: AssetMetrics) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(METRICS_COLLECTION_NAME);
    // let mut docs = Vec::new();
    // for metric in &data {
    //     docs.push(bson::to_document(&metric).unwrap());
    // }
    collection
        .insert_one(bson::to_document(&data).unwrap(), None)
        .unwrap();
}

fn get_1_item_metric_history(
    slug: String,
    metric: String,
    alternate_metric: Option<String>,
    from: String,
    to: String,
    parameters: Option<String>,
) -> (Option<MessariAssetMetrics>, String) {
    let mut headers = header::HeaderMap::new();
    let api_key = env::var("API_KEY").unwrap();
    let mut captype = String::new();
    headers.insert(
        "x-messari-api-key",
        header::HeaderValue::from_str(&api_key).unwrap(),
    );
    let mut url = match &parameters{
        Some(x)=> format!("https://data.messari.io/api/v1/assets/{}/metrics/{}/time-series?start={}&end={}&interval=1d&{}",slug,metric,from,to,x),
        None=> format!("https://data.messari.io/api/v1/assets/{}/metrics/{}/time-series?start={}&end={}&interval=1d",slug,metric,from,to),
    };
    println!("Calling {}", url);
    let client = reqwest::blocking::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let mut json_string = client.get(&url).send().unwrap().text().unwrap();
    ensure that unwrap is not causing problems by matching the result from send(). If an error occurred try to recreate the client
    and issue the last command again. introduce the same measures also for the other send() calls
    //lets check first if we got a rate-limit error
    if json_string.contains("\"error_code\":429") {
        //we exceeded rate limit
        //lets wait6 seconds
        let wait_time = time::Duration::from_secs(7);
        println!(
            "We got an rate exceed error. Waiting for {} seconds",
            wait_time.as_secs()
        );
        thread::sleep(wait_time);
        json_string = client.get(&url).send().unwrap().text().unwrap();
    }

    let v: Option<MessariAssetMetrics> = match serde_json::from_str(&json_string) {
        Ok(x) => {
            captype = String::from(metric);
            Some(x)
        }
        Err(_) => {
            println!("Error with the following metric: {}", metric);
            //lets try the alternate metrics if there is one
            match alternate_metric {
                Some(m) => {
                    //we have to load the same data with the alternate metric
                    url = match parameters{
                        Some(p)=> format!("https://data.messari.io/api/v1/assets/{}/metrics/{}/time-series?start={}&end={}&interval=1d&{}",slug,m,from,to,p),
                        None=> format!("https://data.messari.io/api/v1/assets/{}/metrics/{}/time-series?start={}&end={}&interval=1d",slug,m,from,to),
                    };
                    println!("Calling {}", url);
                    json_string = client.get(&url).send().unwrap().text().unwrap();
                    //lets check first if we got a rate-limit error
                    if json_string.contains("\"error_code\":429") {
                        //we exceeded rate limit
                        //lets wait6 seconds
                        let wait_time = time::Duration::from_secs(7);
                        println!(
                            "We got an rate exceed error. Waiting for {} seconds",
                            wait_time.as_secs()
                        );
                        thread::sleep(wait_time);
                        json_string = client.get(&url).send().unwrap().text().unwrap();
                    }
                    match serde_json::from_str(&json_string) {
                        Ok(x) => {
                            captype = String::from(m);
                            Some(x)
                        }
                        Err(_) => {
                            println!("Error with the following metric: {}", m);
                            None
                        }
                    }
                }
                None => None,
            }
        } //Err Match
    };
    return (v, captype);
}
// pub fn test4(){
//     let client = Client::with_uri_str("mongodb://localhost:27017").unwrap();
//     let database = client.database("mydb");
//     let collection = database.collection("metrics");
//     let mut docs = Vec::new();
//     let mut data = Vec::new();
//     let mut m1 = AssetMetrics{slug: String::from("bitcoin"), timeseries: HashMap::new()};
//     m1.timeseries.insert(String::from("23232323"),MetricsTimeSeries{timestamp: 23232323,mcap: 500.5,price:20000.0});
//     data.push(m1);
//     let mut m2 = AssetMetrics{slug: String::from("bitcoin"), timeseries: HashMap::new()};
//     m2.timeseries.insert(String::from("45455454"),MetricsTimeSeries{timestamp: 45455454,mcap: 500.5,price:20000.0});
//     data.push(m2);

//     for metric in data {
//         // println!("{}", serde_json::to_string(&symbol).unwrap());
//         docs.push(bson::to_document(&metric).unwrap());
//     }
//     // Insert some documents into the "mydb.books" collection.
//     collection.insert_many(docs, None).unwrap();
// }

// fn default_symbol() -> String {
//     "".to_string()
// }

// pub fn test2() {
//     let mut headers = header::HeaderMap::new();
//     let api_key = env::var("API_KEY").unwrap();
//     headers.insert(
//         "x-messari-api-key",
//         header::HeaderValue::from_str(&api_key).unwrap(),
//     );
//     let client = reqwest::blocking::Client::builder()
//         .default_headers(headers)
//         .build()
//         .unwrap();
//     let json_string =
//         client.get("https://data.messari.io/api/v1/assets/axie-infinity/metrics/mcap.circ/time-series?start=2021-07-01&end=2021-07-24&interval=1d")
//         .send()
//         .unwrap()
//         .text()
//             .unwrap();
//     let v: MessariAssetMetrics = serde_json::from_str(&json_string).unwrap();
//     // for (ts, mcap) in v.data.values {
//     //     println!("{}-{}", ts, mcap);
//     // }

//     let j = serde_json::to_string_pretty(&v).unwrap();
//     // Print, write to a file, or send to an HTTP server.
//     println!("{}", j);
// }
