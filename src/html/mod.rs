use fantoccini::{Client, ClientBuilder};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

///Returns the source code of the html file stored at the specified URL. Depending on the parameter `script`
/// the function is using a simple reqwest to get static content or a fantoccini Client in combination with a Chromediver
/// running on the system to load dynamic javascript content.Err
/// # Arguments
/// * `script` - If true the function reads dynamic webpages, static otherwise
/// * `url` - The url of the page to load
/// * `reload`- If true the url is reloaded if the cache is out od date, otherwise it is returned from the cache if already loaded once
/// * `cache` - The cache containing already loaded html sources
pub async fn get_html(
    cache_rc: Arc<Mutex<Cache>>,
    url: &str,
    reload: bool,
    script: bool,
) -> Result<String, ParseError> {
    //first lets check if we have the source already in the cache if we don't need to reload
    //check if data is alread in the cache
    //get a lock on the mutex
    {
        let c = Arc::clone(&cache_rc);
        let cache = c.lock().unwrap();
        //let entry = cache.get(url);
        if let Some(entry) = cache.get(url, reload) {
            return Ok(String::from(&entry.html));
        }
    }
    let source: Result<String, ParseError>;
    if script {
        let mut client = match ClientBuilder::native()
            .connect("http://localhost:9515")
            .await
        {
            Ok(c) => c,
            Err(e) => {
                println!("{}", e);
                return Err(ParseError::new(String::from(
                    "Make sure Chromedriver is started.",
                )));
            }
        };
        source = get_html_source_with_script(&mut client, url).await;
    } else {
        source = get_html_source_no_script(url).await;
    }
    match source {
        Ok(r) => {
            let c = Arc::clone(&cache_rc);
            let mut cache = c.lock().unwrap();
            cache.insert(url, &r);
            Ok(r)
        }
        Err(e) => return Err(e),
    }
}
//     let in_cache = cache.entries.contains_key(url);
//     if !reload && in_cache || (reload && in_cache) {
//         let entry = cache.entries.get(url).unwrap();
//         if !reload || entry.timestamp.elapsed().as_secs() < cache.refresh_after.as_secs() {
//             println!("Getting value from cache");
//             return Ok(String::from(&entry.html));
//         } else {
//             println!("Outdated value in cache. Reloading value");
//         }
//     };
// };

///Actual function to load a static webpage
async fn get_html_source_no_script(
    // cache: Arc<Mutex<Cache>>,
    url: &str,
) -> Result<String, ParseError> {
    let source = reqwest::get(url).await.unwrap().text().await.unwrap();
    // let c = Arc::clone(&cache);
    // let mut cache = c.lock().unwrap();
    // cache.entries.insert(
    //     String::from(url),
    //     CacheEntry {
    //         html: String::from(&source),
    //         timestamp: Instant::now(),
    //     },
    // );
    Ok(source)
}

///Actual function to load the dynamic webpage
async fn get_html_source_with_script(
    client: &mut Client,
    // cache: Arc<Mutex<Cache>>,
    url: &str,
) -> Result<String, ParseError> {
    client.goto(url).await.unwrap();
    let source = client.source().await.unwrap();
    // let c = Arc::clone(&cache);
    // let mut cache = c.lock().unwrap();
    // cache.entries.insert(
    //     String::from(url),
    //     CacheEntry {
    //         html: String::from(&source),
    //         timestamp: Instant::now(),
    //     },
    // );
    Ok(source)
}

///Return the inner html code of a specific tag and its relation in the html tree
/// # Arguments
/// * `regex` - the regex which identifies the tag from which the inner html should be returned
/// * `source` - the source html containing the inner html to be returned
/// * `relation` - A List of `ElementRelation` which are applied to navigate to the desired html content to be returned
pub fn get_inner_html_from_element(
    regex: &str,
    source: &str,
    relations: Vec<Vec<ElementRelation>>,
    symbol: &str,
) -> Result<Vec<String>, ParseError> {
    let re = Regex::new(regex).unwrap();
    let cap = re.captures(source);
    let (tag, attribute) = match cap {
        Some(c) => (String::from(&c[1]), String::from(&c[2])),
        None => {
            // println!("Regex: {}", regex);
            // println!("html: {}", source);
            return Err(ParseError::new(String::from(format!(
                "Element {} for {} not found! Please check manually!!!",
                regex, symbol
            ))));
        }
    };
    let selector = Selector::parse(&format!("{}[{}]", tag, attribute)).unwrap();
    let document = Html::parse_document(&source);
    let r = document.select(&selector).next().unwrap();
    let mut result: Vec<String> = Vec::new();
    for rel in relations {
        let inner = navigate_relation(rel, r);
        result.push(inner.inner_html())
    }
    return Ok(result);
}

///Navigates down a html tree based on the relation passed to the function
fn navigate_relation(rel: Vec<ElementRelation>, element: scraper::ElementRef) -> ElementRef {
    let mut result = element.clone();
    for relation in rel.iter() {
        result = match relation {
            ElementRelation::Parent => ElementRef::wrap(result.parent().unwrap()).unwrap(),
            ElementRelation::Child(i) => {
                if *i < 0 {
                    ElementRef::wrap(result.last_child().unwrap()).unwrap()
                } else if *i == 0 {
                    ElementRef::wrap(result.first_child().unwrap()).unwrap()
                } else {
                    let mut sib = ElementRef::wrap(result.first_child().unwrap()).unwrap();
                    for _ in 0..*i {
                        sib = ElementRef::wrap(sib.next_sibling().unwrap()).unwrap();
                    }
                    sib //for j in
                } //if then else i
            } //ElementRelation::Child
            ElementRelation::Sibling(i) => {
                let mut sib = ElementRef::wrap(result.next_sibling().unwrap()).unwrap();
                for _ in 0..*i {
                    sib = ElementRef::wrap(sib.next_sibling().unwrap()).unwrap();
                }
                sib
            } //ElementRelation::Sibling
        }; //match relation
    } //for relation
    result
} //fn navigate_relation

pub struct HtmlParser {
    pub cache: Arc<Mutex<Cache>>,
}
impl HtmlParser {
    pub async fn new(refresh_after: u64) -> Option<HtmlParser> {
        Some(HtmlParser {
            cache: Arc::new(Mutex::new(Cache::new(refresh_after))),
        })
    }
}

///A simple cache to store `ChacheEntries` and a `Duration` after which an existing item is out-dated
pub struct Cache {
    entries: HashMap<String, CacheEntry>,
    refresh_after: Duration,
}
impl Cache {
    pub fn new(refresh_after: u64) -> Cache {
        Cache {
            entries: HashMap::new(),
            refresh_after: Duration::new(refresh_after, 0),
        }
    }

    pub fn get(&self, key: &str, reload: bool) -> Option<&CacheEntry> {
        match self.entries.get(key) {
            Some(entry) => {
                if reload && entry.timestamp.elapsed().as_secs() > self.refresh_after.as_secs() {
                    return None;
                } else {
                    return Some(entry);
                }
            }
            None => return None,
        };
    }

    pub fn insert(&mut self, key: &str, value: &String) {
        self.entries.insert(
            String::from(key),
            CacheEntry {
                html: String::from(value),
                timestamp: Instant::now(),
            },
        );
    }

    //Todo implement methods to get cacheentries and to set cacheentries to encapsulate "refresh_after" into the cacheentries
    //Implement Out-Of_Date Error to throw if item is outdated
}
///The item which is stored in the cache
//Potentielly mark as private after implementing items above
pub struct CacheEntry {
    html: String,
    timestamp: Instant,
}

///Enumaration to represent the relation inside a html tree
pub enum ElementRelation {
    Parent,
    Child(i32),
    Sibling(i32),
}

pub struct ParseError {
    details: String,
}
impl ParseError {
    pub fn new(details: String) -> ParseError {
        ParseError { details }
    }
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}
impl fmt::Debug for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}
impl Error for ParseError {
    fn description(&self) -> &str {
        &self.details
    }
}
