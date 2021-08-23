use super::assetmomentum::{Asset, AssetMetrics, MetricsTimeSeriesElement};
use core::time::Duration;
use mongodb::bson::{self, doc};
use mongodb::options::{DropCollectionOptions, FindOneOptions, FindOptions, WriteConcern};
use mongodb::{sync::Client, sync::Database};
use std::cmp;
use std::collections::HashMap;

// const DB_NAME: &str = "asset_momentum";
// const DB_URI: &str = "mongodb://localhost:27017";
// const SYMBOLS_COLLECTION_NAME: &str = "symbols";
// const METRICS_COLLECTION_NAME: &str = "metrics";
// const SYMBOLS_IGNORE_COLLECTION_NAME: &str = "ignore_symbols";

pub struct AssetMommentumMongo {
    symbol_collection: String,
    metrics_collection: String,
    ignore_symbols_name: String,
    // client: Client,
    database: Database,
}

impl AssetMommentumMongo {
    pub fn new(
        dbname: &String,
        dburi: &String,
        syname: &String,
        metrname: &String,
        ignsyname: &String,
    ) -> AssetMommentumMongo {
        let client = Client::with_uri_str(&dburi).unwrap();
        let database = client.database(&dbname);
        return AssetMommentumMongo {
            // client: client,
            database: database,
            symbol_collection: String::from(syname),
            metrics_collection: String::from(metrname),
            ignore_symbols_name: String::from(ignsyname),
        };
    }
    // fn init(&mut self) {
    //     if self.client.is_none() || self.database.is_none() {
    //         self.client = Some(Client::with_uri_str(&self.db_uri).unwrap());
    //         self.database = Some(self.client.unwrap().database(&self.db_name));
    //     }
    // }
    pub fn drop_symbol_collection(&self) {
        self.drop_collection(&self.symbol_collection);
    }
    pub fn drop_metric_collections(&self) {
        for (key, _) in self.get_slug_summaries() {
            self.drop_collection(&format!("{}_{}", key, self.metrics_collection));
        }
    }
    pub fn drop_collection(&self, collection_name: &String) {
        // self.init();
        //drop database if existing
        //drop symbols
        let result = self.database.collection(collection_name).drop(
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
    pub fn update_slug_summary(&self, slug: String, first_entry: i64, lastupdate: i64) {
        //get the existing entryprintln
        let summary = self.get_slug_summary(&slug);
        let first_entry = cmp::min(
            match summary {
                Some(asset) => asset.first_entry_sec,
                None => i64::MAX,
            },
            first_entry,
        );
        let collection = self.database.collection(&self.symbol_collection);
        let mut query = bson::Document::new();
        let mut update = bson::Document::new();
        let mut update2 = bson::Document::new();
        update2.insert(String::from("last_update_sec"), lastupdate);
        update2.insert(String::from("first_entry_sec"), first_entry);
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
    pub fn store_symbol(&self, asset: &Asset) {
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.symbol_collection);
        collection
            .insert_one(bson::to_document(asset).unwrap(), None)
            .unwrap();
    }
    pub fn store_symbols(&self, data: &Vec<Asset>) {
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.symbol_collection);
        let mut docs = Vec::new();
        for asset in data {
            // let mut doc = bson::Document::new();
            // doc.insert(String::from("slug"), slug);
            docs.push(bson::to_document(&asset).unwrap());
        }
        collection.insert_many(docs, None).unwrap();
    }
    pub fn store_symbol_to_ignore(&self, slug: &String) {
        let mut doc = bson::Document::new();
        doc.insert(String::from("slug"), slug);
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.ignore_symbols_name);
        collection
            .insert_one(bson::to_document(&doc).unwrap(), None)
            .unwrap();
    }
    pub fn delete_metric(&self, slug: &String, metric: String) {
        let mut doc = bson::Document::new();
        doc.insert(String::from("metrics_id"), metric);
        let collection = self
            .database
            .collection(&format!("{}_{}", slug, self.metrics_collection));
        collection
            .delete_many(bson::to_document(&doc).unwrap(), None)
            .unwrap();
    }

    pub fn _delete_symbol_to_ignore(&self, slug: String) {
        let mut doc = bson::Document::new();
        doc.insert(String::from("slug"), slug);
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.ignore_symbols_name);
        collection
            .delete_one(bson::to_document(&doc).unwrap(), None)
            .unwrap();
    }
    pub fn get_symbol_to_ignore(&self, slug: &String) -> Option<String> {
        let mut doc = bson::Document::new();
        doc.insert(String::from("slug"), slug);
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.ignore_symbols_name);
        let r = collection.find_one(doc, None);
        return match r {
            Ok(result) => match result {
                Some(x) => bson::from_document(x).unwrap(),
                None => None,
            },
            Err(_) => None,
        };
    }
    pub fn get_slug_summary(&self, slug: &String) -> Option<Asset> {
        let mut projection = bson::Document::new();
        projection.insert(String::from("slug"), 1);
        projection.insert(String::from("first_entry_sec"), 1);
        projection.insert(String::from("last_update_sec"), 1);
        // projection.insert(String::from("mcaptype"), 1);
        projection.insert(String::from("_id"), 0);
        let mut doc = bson::Document::new();
        doc.insert(String::from("slug"), slug);
        let find_option = FindOneOptions::builder().projection(projection).build();
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.symbol_collection);
        let r = collection.find_one(doc, find_option);
        match r {
            Ok(doc) => {
                let v: Asset = bson::from_document(doc.unwrap()).unwrap();
                return Some(v);
                // println!("{}-{}", v.slug, v.last_update);
            }
            Err(_e) => {
                println!("Error");
                return None;
            }
        }
    }
    pub fn get_slug_summaries(&self) -> HashMap<String, Asset> {
        //read all slugs from metrics DB
        //for each slug find the highest timestamp
        let mut projection = bson::Document::new();
        projection.insert(String::from("slug"), 1);
        projection.insert(String::from("first_entry_sec"), 1);
        projection.insert(String::from("last_update_sec"), 1);
        // projection.insert(String::from("mcaptype"), 1);
        projection.insert(String::from("_id"), 0);
        let find_option = FindOptions::builder().projection(projection).build();
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.symbol_collection);
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

    pub fn delete_symbol(&self, slug: &String) {
        let mut doc = bson::Document::new();
        doc.insert(String::from("slug"), slug);
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let collection = self.database.collection(&self.symbol_collection);
        collection
            .delete_one(bson::to_document(&doc).unwrap(), None)
            .unwrap();
    }

    pub fn store_metric_elements(&self, slug: &String, metrics: Vec<MetricsTimeSeriesElement>) {
        // println!("Storing metric elements: {}", metrics.len());
        let collection = self
            .database
            .collection(&format!("{}_{}", slug, self.metrics_collection));
        let mut documents = Vec::new();
        for item in metrics {
            documents.push(bson::to_document(&item).unwrap());
        }
        collection.insert_many(documents, None).unwrap();
    }

    pub fn store_asset_metric(&self, metrics: AssetMetrics) {
        self.store_metric_elements(&metrics.slug, metrics.data);
        // let collection = self
        //     .database
        //     .collection(&format!("{}_{}", metrics.slug, self.metrics_collection));
        // let mut documents = Vec::new();
        // for item in metrics.data {
        //     documents.push(bson::to_document(&item).unwrap());
        // }
        // collection.insert_many(documents, None).unwrap();
    }
    pub fn get_metric(
        &self,
        asset: &Asset,
        metric_id: &String,
        from: i64,
        to: Option<i64>,
        sorted: bool,
    ) -> Option<AssetMetrics> {
        let single_value = if to.is_none() { true } else { false };
        let collection = self
            .database
            .collection(&format!("{}_{}", asset.slug, self.metrics_collection));
        let mut projection = bson::Document::new();
        projection.insert(String::from("timestamp_sec"), 1);
        projection.insert(String::from("value"), 1);
        projection.insert(String::from("metrics_id"), 1);
        projection.insert(String::from("_id"), 0);
        let doc: mongodb::bson::Document;
        if single_value {
            doc = doc! {"$and":[{"timestamp_sec":{"$eq": from}},{"metrics_id":{"$eq":metric_id}}]};
        } else {
            doc = doc! {"$and":[{"timestamp_sec":{"$lte": to.unwrap()}},{"timestamp_sec":{"$gte": from}},{"metrics_id":{"$eq":metric_id}}]};
        }
        //bson::Document::new();
        // doc.insert(String::from("metrics_id"), metric_id);
        let find_option = FindOptions::builder().projection(projection).build();
        // let client = Client::with_uri_str(DB_URI).unwrap();
        // let database = client.database(DB_NAME);
        let r = collection.find(doc, find_option);

        let mut result = if single_value {
            AssetMetrics {
                slug: String::from(&asset.slug),
                last_update_sec: from,
                first_item_sec: from,
                data: Vec::new(),
            }
        } else {
            AssetMetrics {
                slug: String::from(&asset.slug),
                last_update_sec: to.unwrap(),
                first_item_sec: from,
                data: Vec::new(),
            }
        };
        match r {
            Ok(iter) => {
                for item in iter {
                    result
                        .data
                        .push(bson::from_document(item.unwrap()).unwrap());
                }
                if sorted {
                    result
                        .data
                        .sort_by(|a, b| a.timestamp_sec.cmp(&b.timestamp_sec));
                }
                Some(result)
            }
            Err(_e) => {
                println!("Error");
                None
            }
        }
    }
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

// pub fn test() {
//     let mut filter = bson::Document::new();
//     let mut ins = bson::Document::new();
//     filter.insert(String::from("slug"), String::from("binance-coin"));

//     ins.insert(
//         String::from("$set"),
//         String::from(r#"{"timeseries.88888000000" :  {"timestamp" : "8888888000000","mcap" : 42393986163.65059,"price" : 1.777777777 }}"#,
//         ),
//     );
//     // let ins = doc! {"$set": {"timeseries.777777000000" :  {"timestamp" : "777777000000","mcap" : 42393986163.65059,"price" : 1.777777777 }}};
//     let client = Client::with_uri_str(DB_URI).unwrap();
//     let database = client.database(DB_NAME);
//     let collection = database.collection(METRICS_COLLECTION_NAME);
//     let result = collection.update_one(filter, ins, None);
//     match result {
//         Ok(r) => println!("Done: {:?}", r),
//         Err(e) => println!("not ok: {:?}", e),
//     }
// }
