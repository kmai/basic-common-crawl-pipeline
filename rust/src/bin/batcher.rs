//! The batcher only operates on index files that contain metadata about the URLs that are part of the crawl.
//! It does not have to download the actual content of the URLs and therefore it does not have to deal with WARC files.
//!
//! For a given crawl, there are hundreds of index files, each containing roughly a gigabyte of URL metadata.
//! Every line in the index file contains the following information. Notice that I have split the line into multiple lines for readability:
//!
//! ```json
//! 0,100,22,165)/
//! 20240722120756
//! {
//!     "url": "http://165.22.100.0/",
//!     "mime": "text/html",
//!     "mime-detected": "text/html",
//!     "status": "301",
//!     "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R",
//!     "length": "689",
//!     "offset": "3499",
//!     "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz",
//!     "redirect": "https://157.245.55.71/"
//! }
//! ```
//!
//! The first lines contains the URL in SURT (Sort-friendly URI Reordering Transform) format, the second lines contains the crawl timestamp, and the remaining lines contain JSON metadata.
//!
//! The URLs in the index files are sorted alpha-numerically.
//!
//! Once the batcher has downloaded (parts of) an index file, it will filter out URLs that are not in English or that did not return a 200 HTTP status code, batch them into groups whose size has a constant upper limit and push the messages containing these URls into a RabbitMQ queue.
use clap::Parser;
use pipeline::commoncrawl::{CdxEntry, ClusterIdxEntry};
use pipeline::rabbitmq::{retry_publish_batch, retry_get_channel_with_queue};
use lazy_static::lazy_static;
use pipeline::{
    commoncrawl::{download_and_unzip, parse_cdx_line, parse_cluster_idx},
    rabbitmq::{BATCH_SIZE, CC_QUEUE_NAME},
    tracing_and_metrics::{run_metrics_server, setup_tracing},
};
use prometheus::{register_int_counter, IntCounter};
use std::fs;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use lapin::Channel;
use futures::future::join_all;
use tokio::sync::Semaphore;

const EXIT_CODE_SUCCESS: i32 = 0;
const EXIT_CODE_FAILURE: i32 = 1;

lazy_static! {
    static ref CLUSTER_IDX_EXCLUDED_ENTRIES_COUNTER: IntCounter = register_int_counter!(
        "crawler_batcher_cluster_idx_excluded_entries",
        "Number of entries from the cluster.idx file that do not match the filter criteria."
    )
    .unwrap();
    static ref CLUSTER_IDX_MATCHING_ENTRIES_COUNTER: IntCounter = register_int_counter!(
        "crawler_batcher_cluster_idx_matching_entries",
        "Number of entries from the cluster.idx file that match the filter criteria."
    )
    .unwrap();
    static ref CLUSTER_IDX_PROCESSED_CHUNKS: IntCounter = register_int_counter!(
        "crawler_batcher_cluster_idx_processed_chunks",
        "Number of chunks from the cluster.idx file that have been processed."
    )
    .unwrap();
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// For an explanation for why this file needs to be provided, please
    /// see Readme.md, section "Why do we download the cluster.idx file up front?".
    #[arg(short='f', long, default_value = "cluster.idx")]
    cluster_idx_filename: String,

    /// This command line argument can be used to limit the number of chunks that should be processed.
    /// If set, the batcher only processes so many lines from the provided cluster.idx file.
    /// Otherwise, it processes all entries in the file.
    #[arg(short='c', long)]
    num_cdx_chunks_to_process: Option<usize>,

    /// This command line argument can be used to specify the crawl version to use.
    /// If set, the batcher will attempt to download the chunks from the specified collection.
    /// Even though it might be outdated, it defaults to CC-MAIN-2024-30.
    #[arg(short='v', long, default_value = "CC-MAIN-2024-30")]
    crawl_version: String,

    /// This command line argument can be used to specify the number of simultaneous chunks to be
    /// processed. It controls the number of Semaphore permits instantiated.
    #[arg(short='p', long)]
    parallelism: Option<usize>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_tracing();
    tokio::task::spawn(run_metrics_server(9000));

    let filename = args.cluster_idx_filename.clone();
    let crawl_version = args.crawl_version.clone();
    let parallelism = args.parallelism.unwrap_or(4);

    tracing::info!(
        "Starting batcher. Cluster Index filename: {} - Max chunks: {:?} - Crawl Version: {} - Parallelism: {} ",
        filename, args.num_cdx_chunks_to_process, crawl_version, parallelism
    );

    let (channel, _queue) = match retry_get_channel_with_queue(3).await {
        Ok(channel_and_queue) => channel_and_queue,
        Err(e) => {
            tracing::error!("{:?}", e);
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };

    // Since we want to do parallel processing, it'd be wise to use atomic reference count
    // to share ownership of the channel.
    let channel = Arc::new(channel);

    let collection = match fs::read_to_string(filename){
        Ok(content) => {
            if let Some(count) = args.num_cdx_chunks_to_process {
                content.lines().filter_map(parse_cluster_idx).take(count).collect::<Vec<_>>()
            } else {
                content.lines().filter_map(parse_cluster_idx).collect::<Vec<_>>()
            }
        },
        Err(e) => {
            tracing::error!("{:?}", e);
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };

    let number_of_chunks = collection.len();

    // We instantiate a semaphore to max out the number of parallel tasks
    let semaphore = Arc::new(Semaphore::new(parallelism));

    // We need an AtomicUsize to avoid race conditions or ownership problems with the borrower.
    let processed = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();

    for cdx_chunk in collection {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let channel = Arc::clone(&channel);
        let crawl_version = crawl_version.clone();
        let processed = Arc::clone(&processed);

        let task = tokio::spawn(async move {
            print!(".");
            process_chunk(cdx_chunk, &channel, &crawl_version).await;
            CLUSTER_IDX_PROCESSED_CHUNKS.inc();
            let current = processed.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::info!("Processed {:.2}% chunks", current as f32 / number_of_chunks as f32 * 100.0);
            drop(permit);
        });

        tasks.push(task);
    }

    join_all(tasks).await;

    tracing::info!("Finished processing {number_of_chunks} chunks from cluster.idx file.");
    std::process::exit(EXIT_CODE_SUCCESS);
}

async fn process_chunk(chunk: ClusterIdxEntry, channel: &Arc<Channel>, crawl_version: &str) {
    // First, download and decompress the data
    let chunk_content = get_chunk_content(chunk, crawl_version)
        .await
        .unwrap();

    // To avoid collecting to a potentially large vector, we can keep the iterator and make it
    // a Peekable. This will allow us to evaluate if the next record is None.
    let mut english_cdx_entries = select_english_entries(&*chunk_content).peekable();

    // Which will loop and create batches, as long as there are more records to add to the
    // current batch (until it reaches BATCH_SIZE).
    while english_cdx_entries.peek().is_some() {
        // We collect only what fills a batch
        let batch = english_cdx_entries
            .by_ref()
            .take(BATCH_SIZE)
            .collect::<Vec<_>>();

        match retry_publish_batch(&channel, CC_QUEUE_NAME, &batch, 3).await {
            Err(e) => {
                tracing::error!("Failed to publish batch: {:?}", e);
                std::process::exit(EXIT_CODE_FAILURE);
            }
            _ => {}
        }
    }
}

async fn get_chunk_content(
    chunk: ClusterIdxEntry,
    collection: &str,
) -> Result<String, FromUtf8Error> {
    String::from_utf8(
        download_and_unzip(
            &format!(
                "https://data.commoncrawl.org/cc-index/collections/{}/indexes/{}",
                collection, chunk.cdx_filename
            ),
            chunk.cdx_offset,
            chunk.cdx_length,
        )
        .await
        .unwrap(),
    )
}

// The returned iterator needs to share the same lifetime as the content, so to keep the borrower
// happy, we make this explicit with &'a.
fn select_english_entries<'a>(content: &'a str) -> impl Iterator<Item = CdxEntry> + 'a {
    content.lines().map(parse_cdx_line).filter(|e| {
        // Then, we iterate to filter entries by the "languages" metadata.
        if let Some(languages) = e.metadata.languages.as_ref() {
            if languages.contains("eng") && e.metadata.status == 200 {
                CLUSTER_IDX_MATCHING_ENTRIES_COUNTER.inc();
                return true
            }
        }
        CLUSTER_IDX_EXCLUDED_ENTRIES_COUNTER.inc();
        false
    })
}

#[cfg(test)]
mod tests {
    use pipeline::commoncrawl::{parse_cdx_line, parse_cluster_idx};

    #[test]
    fn can_parse_cdx_file_with_three_lines() {
        let content = r#"0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}
0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}
0,100,59,139)/ 20240723213521 {"url": "https://139.59.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "5JOQMMSNM6N7UCLGGYXDSPSB3FYAQS2C", "length": "16650", "offset": "64016172", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763518115.82/warc/CC-MAIN-20240723194208-20240723224208-00279.warc.gz", "charset": "UTF-8", "languages": "ind,eng"}"#;
        let cdx: Vec<_> = content.lines().map(parse_cdx_line).collect();
        assert_eq!(cdx.len(), 3);
    }

    #[test]
    fn can_parse_cluster_idx_file_with_four_lines() {
        let content = r#"0,100,22,165)/ 20240722120756   cdx-00000.gz    0       188224  1
101,141,199,66)/robots.txt 20240714155331       cdx-00000.gz    188224  178351  2
104,223,1,100)/ 20240714230020  cdx-00000.gz    366575  178055  3
107,128,254,23)/sites.asp?domain=hydrogenheaters.com 20240725183414     cdx-00000.gz    544630  181599  4"#;
        let cdx_parts: Vec<_> = content.lines().map(parse_cluster_idx).collect();
        assert_eq!(cdx_parts.len(), 4);
    }
}
