use std::hash::Hash;

use iced::futures::channel::mpsc::channel;
use iced::futures::stream::BoxStream;
use iced::Subscription;
use iced_core::event::Status;
use iced_futures::subscription::Recipe;

use crate::{Message, KAFKA_CLIENT};

pub struct TopicSubscription {
    topic: String,
}

impl TopicSubscription {
    pub fn new(topic: String) -> Self {
        TopicSubscription { topic }
    }
}

impl TopicSubscription {
    pub fn create(topic: String) -> Subscription<Message> {
        Subscription::from_recipe(TopicSubscription::new(topic))
    }
}

impl Recipe for TopicSubscription {
    type Output = Message;

    fn hash(&self, state: &mut iced_core::Hasher) {
        std::any::TypeId::of::<Self>().hash(state);
        "TopicSubscription".hash(state)
    }

    fn stream(self: Box<Self>, _: BoxStream<(iced::Event, Status)>) -> BoxStream<Self::Output> {
        let client = KAFKA_CLIENT
            .get()
            .expect("Kafka client should be initialized")
            .clone();

        let (tx, rx) = channel(128);
        client.consume(&self.topic, tx);

        Box::pin(rx)
    }
}
