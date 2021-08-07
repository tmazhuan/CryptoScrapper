use super::assetmomentum::{Asset, AssetMetrics};
use core::time::Duration;
use mongodb::options::{DropCollectionOptions, FindOptions, WriteConcern};
use mongodb::{bson, sync::Client};
use std::collections::HashMap;

const DB_NAME: &str = "asset_momentum";
const DB_URI: &str = "mongodb://localhost:27017";
const SYMBOLS_COLLECTION_NAME: &str = "symbols";
const METRICS_COLLECTION_NAME: &str = "metrics";
const SYMBOLS_IGNORE_COLLECTION_NAME: &str = "ignore_symbols";

pub fn drop_symbol_collection() {
    drop_collection(String::from(SYMBOLS_COLLECTION_NAME));
}
pub fn drop_metric_collections() {
    for (key, _) in get_slugs_with_last_update_date() {
        drop_collection(format!("{}_{}", key, METRICS_COLLECTION_NAME));
    }
}
fn drop_collection(collection_name: String) {
    //drop database if existing
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    //drop symbols
    let result = database.collection(&collection_name).drop(
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
        Ok(_) => println!("Drop of collection {} successful!", collection_name),
        Err(e) => {
            println!(
                "Drop of collection {} not successful!\n{:?}",
                collection_name, e
            );
            return;
        }
    }
}
pub fn update_lastupdate_of_symbol(slug: String, lastupdate: i64) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    let mut query = bson::Document::new();
    let mut update = bson::Document::new();
    let mut update2 = bson::Document::new();
    update2.insert(String::from("last_update"), lastupdate);
    update.insert(String::from("$set"), update2);
    query.insert(String::from("slug"), &slug);
    match collection.update_one(query, update, None) {
        Ok(r) => println!(
            "Update of {} successfull:\nmatched_cound: {} - modified_count: {}",
            &slug, r.matched_count, r.modified_count
        ),
        Err(e) => println!("Update of {} not successfull: {}", &slug, e),
    }
}
pub fn store_symbols(data: &Vec<Asset>) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    let mut docs = Vec::new();
    for asset in data {
        // let mut doc = bson::Document::new();
        // doc.insert(String::from("slug"), slug);
        docs.push(bson::to_document(&asset).unwrap());
    }
    collection.insert_many(docs, None).unwrap();
}
pub fn store_symbol_to_ignore(slug: &String) {
    let mut doc = bson::Document::new();
    doc.insert(String::from("slug"), slug);
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_IGNORE_COLLECTION_NAME);
    collection
        .insert_one(bson::to_document(&doc).unwrap(), None)
        .unwrap();
}

pub fn _delete_symbol_to_ignore(slug: String) {
    let mut doc = bson::Document::new();
    doc.insert(String::from("slug"), slug);
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_IGNORE_COLLECTION_NAME);
    collection
        .delete_one(bson::to_document(&doc).unwrap(), None)
        .unwrap();
}
pub fn get_symbol_to_ignore(slug: &String) -> Option<String> {
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

pub fn get_slugs_with_last_update_date() -> HashMap<String, Asset> {
    //read all slugs from metrics DB
    //for each slug find the highest timestamp
    let mut projection = bson::Document::new();
    projection.insert(String::from("slug"), 1);
    projection.insert(String::from("last_update"), 1);
    projection.insert(String::from("mcaptype"), 1);
    projection.insert(String::from("_id"), 0);
    let find_option = FindOptions::builder().projection(projection).build();
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    let r = collection.find(None, find_option);
    let mut result = HashMap::new();
    match r {
        Ok(iter) => {
            for i in iter {
                let v: Asset = bson::from_document(i.unwrap()).unwrap();
                let key = String::from(&v.slug);
                result.insert(key, v);
                // println!("{}-{}", v.slug, v.last_update);
            }
        }
        Err(_e) => println!("Error"),
    }
    return result;
}

pub fn delete_symbol(slug: &String) {
    let mut doc = bson::Document::new();
    doc.insert(String::from("slug"), slug);
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(SYMBOLS_COLLECTION_NAME);
    collection
        .delete_one(bson::to_document(&doc).unwrap(), None)
        .unwrap();
}

// pub fn _store_metrics(data: Vec<AssetMetrics>) {
//     let client = Client::with_uri_str(DB_URI).unwrap();
//     let database = client.database(DB_NAME);
//     let collection = database.collection(METRICS_COLLECTION_NAME);
//     let mut docs = Vec::new();
//     for metric in &data {
//         docs.push(bson::to_document(&metric).unwrap());
//     }
//     collection.insert_many(docs, None).unwrap();
// }
pub fn store_metric(metrics: AssetMetrics) {
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(&format!("{}_{}", metrics.slug, METRICS_COLLECTION_NAME));
    // let mut projection = bson::Document::new();
    // projection.insert(String::from("slug"), 1);
    // let find_option = FindOptions::builder().projection(projection).build();
    // let r = collection.find(None, find_option);
    // if r.is_ok() {
    //     //we have to distinguish the case where we have already the slug in the collection in which case we only want add the new timeseries
    //     //objects to the object
    // } else {
    //     //or we don't have the slug in the collection in which case we insert the item:
    //     collection
    //         .insert_many(bson::to_document(&data.data).unwrap(), None)
    //         .unwrap();
    // };
    let mut documents = Vec::new();
    for item in metrics.data {
        documents.push(bson::to_document(&item).unwrap());
    }
    collection.insert_many(documents, None).unwrap();
}

pub fn test() {
    let mut filter = bson::Document::new();
    let mut ins = bson::Document::new();
    filter.insert(String::from("slug"), String::from("binance-coin"));

    ins.insert(
        String::from("$set"),
        String::from(r#"{"timeseries.88888000000" :  {"timestamp" : "8888888000000","mcap" : 42393986163.65059,"price" : 1.777777777 }}"#,
        ),
    );
    // let ins = doc! {"$set": {"timeseries.777777000000" :  {"timestamp" : "777777000000","mcap" : 42393986163.65059,"price" : 1.777777777 }}};
    let client = Client::with_uri_str(DB_URI).unwrap();
    let database = client.database(DB_NAME);
    let collection = database.collection(METRICS_COLLECTION_NAME);
    let result = collection.update_one(filter, ins, None);
    match result {
        Ok(r) => println!("Done: {:?}", r),
        Err(e) => println!("not ok: {:?}", e),
    }
}
