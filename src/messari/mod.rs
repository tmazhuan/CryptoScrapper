use super::assetmomentum::{Asset, AssetMetrics, MetricsTimeSeriesElement};
use reqwest::header;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::{thread, time};

const MAX_PAGE_SIZE: i32 = 500;

#[derive(Serialize, Deserialize)]
struct MessariAssetResult {
    data: Vec<HashMap<String, String>>,
}
#[derive(Serialize, Deserialize)]
pub struct MessariStatus {
    elapsed: i32,
    timestamp: String,
}
// #[derive(Serialize, Deserialize)]
// pub struct Symbol {
//     slug: String,
//     // #[serde(default = "default_symbol")]
//     #[serde(deserialize_with = "parse_symbol")]
//     symbol: String,
// }

#[derive(Serialize, Deserialize)]
struct MessariAssetMetrics {
    // status: Status,
    data: MessariMetricsData,
}
#[derive(Serialize, Deserialize)]
struct MessariMetricsData {
    // parameters: MetricsParameter,//we are not interested in the parameters
    // schema: MetricsSchema,//we are not interessted in the MetricsSchema
    schema: MessariMetricsSchema,
    values: Vec<(i64, f64)>,
}
#[derive(Serialize, Deserialize)]
struct MessariMetricsParameter {
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
struct MessariMetricsSchema {
    description: String,
    metric_id: String,
    // minimum_interval: String,
    // source_attribution: Vec<MessariSourceAttribution>,
    // values_schema: Value,
}
#[derive(Serialize, Deserialize)]
struct MessariSourceAttribution {
    name: String,
    url: String,
}

pub fn get_symbols(ranks_to_track: i32) -> Vec<Asset> {
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
    let mut result = Vec::with_capacity((pages * page_size) as usize);
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
            "http://data.messari.io/api/v2/assets?fields=slug&limit={}&page={}",
            page_size,
            p + 1
        );
        println!("Calling {}", url);
        // let resp = reqwest::blocking::get(url).unwrap().text().unwrap();
        // get a client builder
        let resp = client.get(url).send().unwrap().text().unwrap();
        println!("{}", resp);
        let temp: MessariAssetResult = serde_json::from_str(&resp).unwrap();
        // let mut v: AssetResult = AssetResult {
        //     symbols: Vec::with_capacity(temp.data.len()),
        // };
        for i in temp.data {
            result.push(Asset {
                slug: i.get(&String::from("slug")).unwrap().to_string(),
                last_update: 0,
                // mcaptype: None,
            });
        }
        //result.symbols.append(&mut v.symbols);
    }
    // result.symbols.truncate((pages*page_size-(page_size-remainder)) as usize);
    result.truncate(ranks_to_track as usize);
    println!("Length of result: {}", result.len());
    return result;
}

pub fn get_1_item_metric_history(
    slug: String,
    metric: String,
    alternate_metric: Option<String>,
    from: String,
    to: String,
    parameters: Option<String>,
) -> (Option<AssetMetrics>, String) {
    let mut headers = header::HeaderMap::new();
    let api_key = env::var("API_KEY").unwrap();
    let mut metric_id = String::new();
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
    //TODO
    // ensure that unwrap is not causing problems by matching the result from send(). If an error occurred try to recreate the client
    // and issue the last command again. introduce the same measures also for the other send() calls
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
            metric_id = String::from(metric);
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
                            metric_id = String::from(m);
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
    let result = match v {
        Some(r) => {
            let (data, last_update) =
                MetricsTimeSeriesElement::vec_from(r.data.values, String::from(&metric_id));
            Some(AssetMetrics {
                slug: slug,
                data: data,
                last_update: last_update,
            })
        }
        None => None,
    };
    return (result, metric_id);
}
