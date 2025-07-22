//! This module contains helper functions to interact with the RabbitMQ service.
use std::time::Duration;

use crate::commoncrawl::CdxEntry;
use anyhow::Context;
use lapin::publisher_confirm::PublisherConfirm;
use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Queue,
};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

pub const BATCH_SIZE: usize = 1000;
pub const CC_QUEUE_NAME: &str = "batches";
const RABBIT_MQ_TIMEOUT: Duration = Duration::from_secs(20);

/// Tries to get the environment variable `RABBITMQ_CONNECTION_STRING` and panics if not found.
pub fn get_rabbitmq_connection_string() -> String {
    std::env::var("RABBITMQ_CONNECTION_STRING").expect("RABBITMQ_CONNECTION_STRING must be set.")
}

/// Creates a RabbitMQ connection with default [ConnectionProperties].
/// Can return timeout errors if the operation times out.
#[tracing::instrument]
pub async fn rabbitmq_connection() -> Result<Connection, anyhow::Error> {
    let connection_string = get_rabbitmq_connection_string();

    let connection = tokio::time::timeout(
        RABBIT_MQ_TIMEOUT,
        Connection::connect(&connection_string, ConnectionProperties::default()),
    )
    .await
    .context("Timed out while trying to connect to RabbitMQ")?;

    connection.with_context(|| "Failed to connect to RabbitMQ")
}

/// Attempts to create a RabbitMQ connection with default [ConnectionProperties].
/// Will back-off exponentially based on the `retries` parameter while introducing jitter.
#[tracing::instrument]
pub async fn rabbitmq_connection_with_retry(retries: usize) -> Result<Connection, anyhow::Error> {
    let retry_strategy = ExponentialBackoff::from_millis(10)
        .map(jitter)
        .take(retries);

    Retry::spawn(retry_strategy, || async {
        tracing::info!("Creating a RabbitMQ connection..");
        match rabbitmq_connection().await {
            Ok(conn) => Ok(conn),
            Err(err) => {
                tracing::warn!("RabbitMQ connection attempt failed.");
                Err(err)
            }
        }
    })
    .await
    .with_context(|| format!("All {retries} RabbitMQ connection attempts failed"))
}

/// Creates a channel and a queue using the functions
/// [rabbitmq_channel] and [rabbitmq_declare_queue].
#[tracing::instrument]
pub async fn rabbitmq_channel_with_queue(
    conn: &Connection,
    queue_name: &str,
) -> Result<(Channel, Queue), anyhow::Error> {
    let channel = rabbitmq_channel(conn).await?;
    let queue = rabbitmq_declare_queue(&channel, queue_name, FieldTable::default()).await?;
    Ok((channel, queue))
}

/// Declares a queue on a given channel. Arguments can be provided but
/// the [QueueDeclareOptions] are set to default.
pub async fn rabbitmq_declare_queue(
    channel: &Channel,
    queue_name: &str,
    arguments: FieldTable,
) -> Result<Queue, anyhow::Error> {
    let queue = tokio::time::timeout(
        RABBIT_MQ_TIMEOUT,
        channel.queue_declare(queue_name, QueueDeclareOptions::default(), arguments),
    )
    .await
    .context("Timed out while trying to declare a RabbitMQ queue")?
    .context("Failed to declare RabbitMQ queue")?;

    Ok(queue)
}

/// Creates a RabbitMQ channel and sets the prefetch count to 1.
/// Can raise timeout errors if connections time out.
pub async fn rabbitmq_channel(conn: &Connection) -> Result<Channel, anyhow::Error> {
    let channel = tokio::time::timeout(RABBIT_MQ_TIMEOUT, conn.create_channel())
        .await
        .context("Timed out while trying to create a RabbitMQ channel")?
        .context("Failed to create RabbitMQ channel")?;

    tokio::time::timeout(
        RABBIT_MQ_TIMEOUT,
        channel.basic_qos(1, BasicQosOptions::default()),
    )
    .await
    .context("Timed out while trying to set QoS on the channel")?
    .context("Failed to set QoS on the channel")?;
    Ok(channel)
}

/// Creates a RabbitMQ consumer based on a channel, a queue name and a consumer_tag.
/// Uses default [BasicConsumeOptions] and [FieldTable].
pub async fn rabbitmq_consumer(
    channel: &Channel,
    queue_name: &str,
    consumer_tag: &str,
) -> Result<lapin::Consumer, anyhow::Error> {
    let consumer = tokio::time::timeout(
        RABBIT_MQ_TIMEOUT,
        channel.basic_consume(
            queue_name,
            consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ),
    )
    .await
    .context("Timed out while trying to consume from a RabbitMQ queue")??;

    Ok(consumer)
}

/// Publishes a batch to a given queue using default [BasicPublishOptions] and [BasicProperties].
/// Panics in case of an error.
pub async fn publish_batch(
    channel: &Channel,
    queue_name: &str,
    batch: &[CdxEntry],
) -> Result<PublisherConfirm, anyhow::Error> {
    tracing::info!("Sending a batch of {} entries", batch.len());
    channel
        .basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            &serde_json::to_vec(&batch).unwrap(),
            BasicProperties::default(),
        )
        .await
        .context("rabbitmq basic publish")
}

pub async fn retry_publish_batch(
    channel: &Channel,
    queue_name: &str,
    batch: &[CdxEntry],
    retries: usize,
) -> Result<PublisherConfirm, anyhow::Error> {
    let retry_strategy = ExponentialBackoff::from_millis(10)
        // Jitter always looks nice as it helps having all clients reconnect at once
        .map(jitter)
        .take(retries);

    Retry::spawn(retry_strategy, || async {
        publish_batch(channel, queue_name, batch).await
    })
    .await
}
