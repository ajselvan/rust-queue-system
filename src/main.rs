use dotenv::dotenv;
use lapin::{
    options::*,
    types::FieldTable,
    BasicProperties,
    Channel,
    Connection,
    ConnectionProperties,
    ExchangeKind,
};
use serde_json::Value;
use sqlx::postgres::PgListener;
use std::{env, time::Duration};
use tokio::{signal, time::sleep};

#[derive(Debug)]
struct Config {
    amqp_uri: String,
    position_exchange: String,
    orders_exchange: String,
    database_url: String,
    listen_channel: String,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            amqp_uri: env::var("RABBITMQ_URI")?,
            position_exchange: env::var("RABBITMQ_POSITION_EXCHANGE")?,
            orders_exchange: env::var("RABBITMQ_ORDERS_EXCHANGE")?,
            database_url: env::var("DATABASE_URL")?,
            listen_channel: env::var("PG_LISTEN_CHANNEL")?,
        })
    }
}

async fn connect_rabbitmq_forever(uri: &str) -> Connection {
    loop {
        match Connection::connect(uri, ConnectionProperties::default()).await {
            Ok(conn) => return conn,
            Err(e) => {
                eprintln!("🔁 RabbitMQ connect error: {}. Retrying in 5s...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn run_listener(cfg: &Config, channel: Channel) {
    loop {
        let result = async {
            let mut listener = PgListener::connect(&cfg.database_url).await?;
            listener.listen(&cfg.listen_channel).await?;
            println!("✅ Listening for DB notifications on '{}'", cfg.listen_channel);

            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload_str = notification.payload();
                        println!("📥 Received notification: {}", payload_str);

                        if let Ok(payload_json) = serde_json::from_str::<Value>(payload_str) {
                            if let Some(topic) = payload_json.get("topic").and_then(|t| t.as_str()) {
                                let valid_topics = [
                                    "full_position",
                                    "position_deletion",
                                    "full_open_orders",
                                    "open_orders_deletion",
                                ];

                                if valid_topics.contains(&topic) {
                                    if let Some(address) = payload_json.get("address").and_then(|a| a.as_str()) {
                                        let exchange_name = match topic {
                                            "full_position" | "position_deletion" => &cfg.position_exchange,
                                            "full_open_orders" | "open_orders_deletion" => &cfg.orders_exchange,
                                            _ => continue, // should never hit
                                        };

                                        channel
                                            .basic_publish(
                                                exchange_name,
                                                address,
                                                BasicPublishOptions::default(),
                                                payload_str.as_bytes(),
                                                BasicProperties::default().with_delivery_mode(2),
                                            )
                                            .await?
                                            .await?;

                                        println!(
                                            "🚀 Published to RabbitMQ [exchange: {}, routing_key: {}] (topic: {})",
                                            exchange_name, address, topic
                                        );
                                    } else {
                                        println!("⚠️ Skipping message: address missing");
                                    }
                                } else {
                                    println!("⚠️ Skipping message: unexpected topic '{}'", topic);
                                }
                            } else {
                                println!("⚠️ Skipping message: no topic found");
                            }
                        } else {
                            eprintln!("❌ Invalid JSON in payload: {}", payload_str);
                        }
                    }
                    Err(err) => {
                        eprintln!("❌ Listener error: {}. Reconnecting...", err);
                        break;
                    }
                }
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        if let Err(e) = result {
            eprintln!("🔄 Listener crashed: {}. Retrying in 5s...", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let cfg = Config::from_env()?;

    let conn = connect_rabbitmq_forever(&cfg.amqp_uri).await;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            &cfg.position_exchange,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .exchange_declare(
            &cfg.orders_exchange,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    println!("📡 RabbitMQ exchanges declared: {}, {}", cfg.position_exchange, cfg.orders_exchange);

    tokio::select! {
        _ = run_listener(&cfg, channel.clone()) => {},
        _ = signal::ctrl_c() => {
            println!("🛑 Received shutdown signal. Exiting...");
        }
    }

    Ok(())
}
