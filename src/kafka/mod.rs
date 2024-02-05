use std::time::Duration;

use iced::futures::channel::mpsc::Sender;
use iced::futures::TryStreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, ClientContext, Message};
use tracing::info;

#[derive(Clone)]
pub struct BoudinClient {
    hosts: Vec<String>,
}

pub struct BoudinContext;

impl ClientContext for BoudinContext {}

impl ConsumerContext for BoudinContext {
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        info!("pre rebalance {:?}", rebalance);
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        info!("post rebalance {:?}", rebalance);
    }
}

type BoudinConsumer = StreamConsumer<BoudinContext>;

impl BoudinClient {
    pub fn new(broker: &str) -> Self {
        Self {
            hosts: vec![broker.to_string()],
        }
    }

    pub fn consume(&self, topic: &str, mut tx: Sender<crate::Message>) {
        let context = BoudinContext;
        let consumer: BoudinConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("enable.auto.commit", "false")
            .set("group.id", "toto")
            // See: https://github.com/fede1024/rust-rdkafka/issues/499
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        let result = consumer.fetch_metadata(Some(topic), Duration::from_secs(1));
        for topic in result.unwrap().topics() {
            for partition in topic.partitions() {
                println!("{}", partition.id());
            }
        }

        let mut partition_list = TopicPartitionList::new();
        partition_list.add_partition_offset(topic, 0, Offset::Beginning);

        consumer
            .assign(&partition_list)
            .expect("Can't subscribe to specified topics");

        tokio::spawn(async move {
            consumer
                .stream()
                .try_for_each(|message| {
                    let mut tx = tx.clone();
                    let message = message.detach();
                    async move {
                        let digest = message.key().zip(message.payload()).map(|(key, value)| {
                            let key = String::from_utf8_lossy(key);
                            let value = String::from_utf8_lossy(value);
                            format!("{}: {}", key, value)
                        });
                        if let Some(digest) = digest {
                            println!("Digest {}", digest);
                            if let Err(err) = tx.try_send(crate::Message::TopicMessage(digest)) {
                                println!("{}", err);
                            }
                        }

                        Ok(())
                    }
                })
                .await
        });
    }

    pub(crate) fn list_topics(&self) -> Vec<String> {
        let context = BoudinContext;
        let client_config: BoudinConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("group.id", "toto")
            // See: https://github.com/fede1024/rust-rdkafka/issues/499
            .set("broker.version.fallback", "0.10.2.1")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        let topics = client_config
            .fetch_metadata(None, Duration::from_secs(2))
            .expect("metadata");

        topics
            .topics()
            .into_iter()
            .map(|topic| topic.name().to_string())
            .collect()
    }
}