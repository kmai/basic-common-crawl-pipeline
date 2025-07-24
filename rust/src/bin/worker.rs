//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way nor does it output the extracted text to a file.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use clap::Parser;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};
use tokio::sync::Semaphore;
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry},
    filter::ContentFilter,
    rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing},
    trafilatura,
};
use warc::WarcHeader;
use pipeline::filter::FilterResult;

lazy_static! {
    static ref RABBITMQ_CONNECTION_ERRORS_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_rabbitmq_connection_errors",
        "Number of RabbitMQ connection errors."
    )
    .unwrap();

    // Batch metrics
    static ref BATCHES_RECEIVED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_batches_received",
        "Number of batches received by the worker."
    )
    .unwrap();

    static ref BATCHES_PROCESSED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_batches_processed",
        "Number of batches processed by the worker."
    )
    .unwrap();


    // Entry metrics
    static ref ENTRIES_RECEIVED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_entries_received",
        "Number of entries received by the worker."
    )
    .unwrap();

    static ref ENTRIES_PROCESSED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_entries_processed",
        "Number of entries processed by the worker."
    )
    .unwrap();

    // WARC Entry metrics
    static ref WARC_ENTRIES_PARSED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_parsed",
        "Number of WARC entries parsed from events by the worker."
    )
    .unwrap();

    static ref WARC_ENTRIES_EXTRACTED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_extracted",
        "Number of WARC entries extracted by the worker."
    )
    .unwrap();

    static ref WARC_ENTRIES_EXTRACTION_ERRORS_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_extraction_errors",
        "Number of WARC entry extraction errors by the worker."
    )
    .unwrap();

    static ref WARC_ENTRIES_TOO_SHORT_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_too_short",
        "Number of WARC entries filtered out for content being too short."
    )
    .unwrap();

    static ref WARC_ENTRIES_TOO_LONG_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_too_long",
        "Number of WARC entries filtered out for content being too long."
    )
    .unwrap();

    static ref WARC_ENTRIES_ACCEPTED_COUNTER: IntCounter = register_int_counter!(
        "crawler_worker_warc_accepted",
        "Number of WARC entries that passed all filters."
    )
    .unwrap();

}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// This command line argument can be used to specify the number of simultaneous chunks to be
    /// processed. It controls the number of Semaphore permits instantiated.
    #[arg(short='p', long)]
    parallelism: Option<usize>,

    // This argument can be used to specify the minimum content length
    #[arg(short='m', long, default_value = "500")]
    min_content_length: Option<usize>,

    // This argument can be used to specify the maximum content length
    #[arg(short='M', long, default_value = "1000000")]
    max_content_length: Option<usize>,

    // This argument can be used to disable the content filter.
    #[arg(long)]
    disable_content_filter: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let parallelism = args.parallelism.unwrap_or(4);

    setup_tracing();
    tokio::task::spawn(run_metrics_server(9001));

    // Create content filter based on args
    let content_filter = if args.disable_content_filter {
        None
    } else {
        ContentFilter::new(args.min_content_length, args.max_content_length)
    };

    tracing::info!("Starting worker. Parallelism: {parallelism}, Filter: {content_filter:?}, waiting");
    tracing::info!("Waiting for messages on queue {CC_QUEUE_NAME}");
    let rabbit_conn = rabbitmq_connection().await.unwrap();
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME)
        .await
        .unwrap();
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME, "worker")
        .await
        .unwrap();
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data);
                let batch_size = batch.as_ref().unwrap().len();

                BATCHES_RECEIVED_COUNTER.inc();
                ENTRIES_RECEIVED_COUNTER.inc_by(batch_size as u64);

                tracing::info!("Received a batch of {batch_size} entries");
                process_batch(batch.unwrap(), content_filter.clone(), parallelism).await;

                delivery.ack(BasicAckOptions::default()).await.unwrap();
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "Failed to receive message from RabbitMQ. Reconnecting.");
                RABBITMQ_CONNECTION_ERRORS_COUNTER.inc();
                continue;
            }
        }
    }
}

async fn process_batch(batch: Vec<CdxEntry>, content_filter: Option<ContentFilter>, parallelism: usize) {
    let semaphore = Arc::new(Semaphore::new(parallelism));
    let processed_entry_count = Arc::new(AtomicUsize::new(0));
    let batch_size = batch.len();
    let mut tasks = Vec::with_capacity(batch_size);

    for entry in batch {
        let processed = processed_entry_count.clone();
        let semaphore_clone = semaphore.clone();
        let content_filter_clone = content_filter.clone(); // Clone the Option<ContentFilter>

        let task = tokio::spawn(async move {
            let permit = semaphore_clone.acquire_owned().await.unwrap();
            if let Err(e) = process_entry(entry, content_filter_clone).await {
                tracing::warn!("Failed to process entry: {:?}", e);
            }
            let current = processed.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::info!("Processed {:.2}% entries", current as f32 / batch_size as f32 * 100.0);
            drop(permit);
        });
        tasks.push(task);
    }

    let mut results = FuturesUnordered::from_iter(tasks);
    while let Some(res) = results.next().await {
        if let Err(e) = res {
            tracing::error!("Task panicked or was cancelled: {:?}", e);
        }
    }

    BATCHES_PROCESSED_COUNTER.inc();
}

async fn process_entry(entry: CdxEntry, content_filter: Option<ContentFilter>) -> Result<(), anyhow::Error> {
    let data = download_and_unzip(
        &format!("https://data.commoncrawl.org/{}", entry.metadata.filename),
        entry.metadata.offset,
        entry.metadata.length,
    ).await?;

    for warc_entry in warc::WarcReader::new(data.as_slice()).iter_records() {
        let warc_entry = warc_entry?;
        if warc_entry.header(WarcHeader::WarcType).unwrap() != "response" {
            continue;
        }

        tracing::info!(
            "Successfully read WARC entry with URL {}",
            warc_entry.header(WarcHeader::TargetURI).unwrap()
        );

        let raw_content = String::from_utf8_lossy(warc_entry.body());
        let html_begin_index = raw_content.find("\n\n");
        let Some(html_begin_index) = html_begin_index else {
            tracing::warn!("Failed to find HTML content in WARC entry");
            continue;
        };
        WARC_ENTRIES_PARSED_COUNTER.inc();

        tracing::debug!("First 2000 characters of raw content: {}", &raw_content[..2000]);

        let content = trafilatura::extract(&raw_content[html_begin_index..]).await?;

        if let Some(content) = content {
            let content_length = content.len();
            tracing::info!("Extracted content of length {}", content_length);

            // Apply content filter if present
            if let Some(ref filter) = content_filter {
                match filter.check(&content) {
                    FilterResult::Accept => {
                        tracing::info!("Content accepted ({} chars)", content_length);
                        WARC_ENTRIES_EXTRACTED_COUNTER.inc();
                        WARC_ENTRIES_ACCEPTED_COUNTER.inc();

                        // Process the accepted content here
                        tracing::debug!("Extracted content: {}", &content);
                    },
                    FilterResult::TooShort => {
                        tracing::debug!("Content too short ({} chars), skipping", content_length);
                        WARC_ENTRIES_TOO_SHORT_COUNTER.inc();
                        continue;
                    },
                    FilterResult::TooLong => {
                        tracing::debug!("Content too long ({} chars), skipping", content_length);
                        WARC_ENTRIES_TOO_LONG_COUNTER.inc();
                        continue;
                    }
                }
            } else {
                // No filter, accept all content
                tracing::info!("Content accepted ({} chars) - no filter", content_length);
                WARC_ENTRIES_EXTRACTED_COUNTER.inc();
                WARC_ENTRIES_ACCEPTED_COUNTER.inc();
                tracing::debug!("Extracted content: {}", &content);
            }
        } else {
            tracing::warn!("Failed to extract content from WARC entry");
            WARC_ENTRIES_EXTRACTION_ERRORS_COUNTER.inc();
        }
    }

    ENTRIES_PROCESSED_COUNTER.inc();
    Ok(())
}
