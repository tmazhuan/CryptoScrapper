use super::db;
use super::messari;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Asset {
    pub slug: String,
    pub last_update: i64,
    // pub mcaptype: Option<String>,
}
// pub struct AssetMetric {
//     pub slug: String,
//     pub values: Vec<(i64, f64)>,
//     pub metric_id: String,
// }
pub struct AssetMetrics {
    pub slug: String,
    pub last_update: i64,
    pub data: Vec<MetricsTimeSeriesElement>,
}

#[derive(Serialize, Deserialize)]
pub struct MetricsTimeSeriesElement {
    pub timestamp: i64,
    pub value: f64,
    pub metrics_id: String,
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
                timestamp: t,
                value: v,
                metrics_id: String::from(&metrics_id),
            });
            if t > maxtimestamp {
                maxtimestamp = t;
            }
        }
        return (result, maxtimestamp);
    }
}

// #[derive(Serialize, Deserialize)]
// pub struct AssetLastUpdated {
//     pub slug: String,
//     pub last_update: i64,
//     pub mcaptype: Option<String>,
// }
pub fn test() {
    let s = String::from("bitcoin");
    db::store_symbol_to_ignore(&s);
}
pub fn test2() {
    let x = db::get_symbol_to_ignore(&String::from("bitcoin"));
    match x {
        Some(symbol) => println!("{}", symbol),
        None => println!("Symbol not found"),
    }
}
pub fn test3() {
    db::get_slugs_with_last_update_date();
}

pub fn test4() {
    db::test();
}

pub fn update_asset_momentum(ranks_to_track: i32) {
    //get top x assets
    let messari_assets = messari::get_symbols(ranks_to_track);
    //filter out the symbols to ignore
    //lets remove the assets we want to ignore
    let messari_assets_filtered: Vec<Asset> = messari_assets
        .into_iter()
        .filter(|asset| db::get_symbol_to_ignore(&asset.slug).is_none())
        .collect();
    //create two groups with the remaining assets
    let db_assets = db::get_slugs_with_last_update_date();
    let mut assets_to_update: Vec<Asset> = Vec::new();
    for ma in messari_assets_filtered {
        let da = db_assets.get(&ma.slug);
        match da {
            Some(a) => assets_to_update.push(Asset {
                slug: String::from(&a.slug),
                last_update: a.last_update,
                // mcaptype: match &a.mcaptype {
                //     Some(t) => Some(String::from(t)),
                //     None => None,
                // },
            }),
            None => assets_to_update.push(ma),
        }
        // match db_assets.iter().position(|x| x.slug == ma.slug) {
        //     //one group are already existing assets
        //     Some(p) => assets_to_update.push(ma),
        //     //     AssetLastUpdated {
        //     //     slug: a.slug,
        //     //     last_update: metric_assets[p].last_update,
        //     //     mcaptype: metric_assets[p].mcaptype.clone(),
        //     // }),
        //     //one group are completely new assets not yet in the metrics DB
        //     None => assets_to_update.push(AssetLastUpdated {
        //         slug: ma.slug,
        //         last_update: 0,
        //         mcaptype: None,
        //     }),
        // }
    }
    // update_assets(assets_to_update, NaiveDate::from_ymd(2021, 8, 2));
}
/*fn update_assets(assets_to_update: Vec<Asset>, to_date: NaiveDate) {
    for asset in assets_to_update {
        let (price, _) = messari::get_1_item_metric_history(
            String::from(&asset.slug),
            String::from("price"),
            None,
            format!(
                "{}",
                Utc.timestamp(asset.last_update / 1000, 0)
                    .format("%Y-%m-%d")
            ),
            to_date.format("%Y-%m-%d").to_string(),
            Some(String::from("columns=close")),
        );
        let (mcap_primary, mcap_secondary) = match asset.mcaptype {
            Some(x) => (x, None),
            None => (String::from("mcap.out"), Some(String::from("mcap.circ"))),
        };
        let (mcap, metric_id) = messari::get_1_item_metric_history(
            String::from(&asset.slug),
            mcap_primary,
            mcap_secondary,
            format!(
                "{}",
                Utc.timestamp(asset.last_update / 1000, 0)
                    .format("%Y-%m-%d")
            ),
            to_date.format("%Y-%m-%d").to_string(),
            None,
        );
        // check if we get a SOME back from our api. if one of the metrics is NONE we ignore the slug
        // to ensure, that we remember the slugs to ignore we write the to a new collection
        // and delete the slug from the symbols database
        if price.is_none() || mcap.is_none() {
            //add the slug to our ignore collection
            db::store_symbol_to_ignore(&asset.slug);
            //delete the slug from our symbols database
            db::delete_symbol(&asset.slug);
            //go to the next symbol
            continue;
        }
        db::store_metric(combine_metrics(
            String::from(&asset.slug),
            to_hash_map(price.unwrap().values),
            to_hash_map(mcap.unwrap().values),
            metric_id,
        ));
    }
}*/
pub fn init_asset_momentum(ranks_to_track: i32, from: String, to: String) {
    //drop Symbols collection if existing
    db::drop_metric_collections();
    db::drop_symbol_collection();
    //get symbol/slug combination
    let messari_assets = messari::get_symbols(ranks_to_track);
    //lets remove the assets we want to ignore
    let messari_assets_filterd: Vec<Asset> = messari_assets
        .into_iter()
        .filter(|asset| db::get_symbol_to_ignore(&asset.slug).is_none())
        .collect();
    db::store_symbols(&messari_assets_filterd);
    //for each symbol load the price data
    //let mut to_store = Vec::new();
    for asset in messari_assets_filterd {
        let (price, _) = messari::get_1_item_metric_history(
            String::from(&asset.slug),
            String::from("price"),
            None,
            from.clone(),
            to.clone(),
            Some(String::from("columns=close")),
        );
        // let (mcap, _) = messari::get_1_item_metric_history(
        //     String::from(&asset.slug),
        //     String::from("mcap.out"),
        //     Some(String::from("mcap.circ")),
        //     from.clone(),
        //     to.clone(),
        //     None,
        // );
        // check if we get a SOME back from our api. if one of the metrics is NONE we ignore the slug
        // to ensure, that we remember the slugs to ignore we write the to a new collection
        // and delete the slug from the symbols database
        // if price.is_none() || mcap.is_none() {
        if price.is_none() {
            //add the slug to our ignore collection
            db::store_symbol_to_ignore(&asset.slug);
            //delete the slug from our symbols database
            db::delete_symbol(&asset.slug);
            //go to the next symbol
            continue;
        }
        let metric = price.unwrap();
        let slug = String::from(&metric.slug);
        let lastupdate = metric.last_update;
        db::store_metric(metric);
        db::update_lastupdate_of_symbol(slug, lastupdate);
        // db::store_metric(mcap.unwrap());
        // db::store_metric(combine_metrics(
        //     String::from(&asset.slug),
        //     to_hash_map(price.unwrap().values),
        //     to_hash_map(mcap.unwrap().values),
        //     metric_id,
        // ));
    }
}

// fn to_hash_map(input: Vec<(i64, f64)>) -> HashMap<String, f64> {
//     let mut result = HashMap::new();
//     for (timestamp, value) in input {
//         result.insert(timestamp.to_string(), value);
//     }
//     return result;
// }

// fn combine_metrics(
//     slug: String,
//     price: HashMap<String, f64>,
//     mcap: HashMap<String, f64>,
//     mcaptype: String,
// ) -> AssetMetrics {
//     let mut result = AssetMetrics {
//         slug: slug,
//         last_update: 0,
//         timeseries: HashMap::new(),
//         mcaptype: mcaptype,
//     };
//     let mut max = 0;
//     for (timestamp, price_value) in price {
//         let temp = timestamp.parse::<i64>().unwrap();
//         if temp > max {
//             max = temp;
//         };
//         result.timeseries.insert(
//             String::from(&timestamp),
//             MetricsTimeSeriesElement {
//                 timestamp: String::from(&timestamp).parse::<i64>().unwrap(),
//                 price: price_value,
//                 mcap: *mcap.get(&timestamp).unwrap_or(&0.0f64),
//             },
//         );
//     }
//     result.last_update = max;
//     return result;
// }

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
