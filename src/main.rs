use iced::futures::StreamExt;
use iced::widget::{button, column, container, row, scrollable, text, text_input, Column};
use iced::{Alignment, Application, Command, Element, Renderer, Settings};
use iced_futures::Subscription;
use once_cell::sync::{Lazy, OnceCell};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::kafka::BoudinClient;
use crate::subscriptions::topic_subscription::TopicSubscription;

mod kafka;
mod subscriptions;

static KAFKA_CLIENT: OnceCell<BoudinClient> = OnceCell::new();

pub fn main() -> iced::Result {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "boudin=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    Boudin::run(Settings::default())
}

enum Boudin {
    Connecting {
        input_value: String,
    },
    Connected {
        connected_to: Vec<String>,
        topics: Vec<String>,
    },
    TopicView {
        topic: String,
        messages: Vec<String>,
    },
}

static INPUT_ID: Lazy<text_input::Id> = Lazy::new(text_input::Id::unique);
static SCROLL_ID: Lazy<scrollable::Id> = Lazy::new(scrollable::Id::unique);

#[derive(Debug, Clone)]
pub enum Message {
    HostInputChanged(String),
    ConnectToHosts(String),
    Topics(Vec<String>),
    OpenTopic(String),
    TopicMessage(String),
}

impl Application for Boudin {
    type Message = Message;
    type Theme = iced::theme::Theme;
    type Executor = iced::executor::Default;
    type Flags = ();

    fn new(_flags: ()) -> (Boudin, Command<Message>) {
        (
            Boudin::Connecting {
                input_value: "Enter hosts".to_string(),
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        String::from("Counter - Iced")
    }

    fn update(&mut self, message: Message) -> Command<Message> {
        match message {
            Message::ConnectToHosts(hosts) => {
                let hosts: Vec<_> = hosts.split(",").map(|host| host.to_string()).collect();
                KAFKA_CLIENT.get_or_init(|| BoudinClient::new(&hosts.join(",")));
                *self = Boudin::Connected {
                    connected_to: hosts,
                    topics: vec![],
                };
                Command::perform(on_kafka_client_connected(), Message::Topics)
            }
            Message::Topics(host_topics) => {
                println!("Got topics: {:?}", host_topics);
                if let Boudin::Connected { topics, .. } = self {
                    *topics = host_topics;
                }

                Command::none()
            }
            Message::HostInputChanged(hosts) => {
                if let Boudin::Connecting { input_value } = self {
                    *input_value = hosts;
                }

                Command::none()
            }
            Message::OpenTopic(topic) => {
                *self = Boudin::TopicView {
                    topic,
                    messages: vec![],
                };
                Command::none()
            }
            Message::TopicMessage(message) => {
                println!("{}", message);
                if let Boudin::TopicView { messages, .. } = self {
                    messages.push(message)
                }

                Command::none()
            }
        }
    }

    fn view(&self) -> Element<Message> {
        match self {
            Boudin::Connecting { input_value } => {
                let input = text_input("enter hosts", input_value)
                    .id(INPUT_ID.clone())
                    .on_input(Message::HostInputChanged)
                    .on_submit(Message::ConnectToHosts(input_value.clone()))
                    .padding(15)
                    .size(30);

                column![input].into()
            }
            Boudin::Connected { topics, .. } => {
                let column = Column::with_children(
                    topics
                        .iter()
                        .map(|topic| {
                            container(
                                button(text(topic)).on_press(Message::OpenTopic(topic.clone())),
                            )
                            .into()
                        })
                        .collect(),
                );

                column
                    .align_items(Alignment::Start)
                    .spacing(10)
                    .padding(20)
                    .into()
            }
            Boudin::TopicView { topic, messages } => {
                println!("{}", topic);
                println!("{:?}", messages);

                let rows: Vec<Element<Message, Renderer>> = messages
                    .into_iter()
                    .map(|message| row(vec![text(message).into()]).into())
                    .collect();

                column!(text(topic), scrollable(column(rows)).id(SCROLL_ID.clone())).into()
            }
        }
    }

    fn subscription(&self) -> Subscription<Self::Message> {
        let mut subs = vec![];
        match self {
            Boudin::Connecting { .. } => {}
            Boudin::Connected { .. } => {}
            Boudin::TopicView { topic, .. } => subs.push(TopicSubscription::create(topic.clone())),
        }

        Subscription::batch(subs)
    }
}

async fn on_kafka_client_connected() -> Vec<String> {
    let client = KAFKA_CLIENT
        .get()
        .expect("Kafka client should be connected");

    client.list_topics()
}
