use futures_util::stream::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let conn = Connection::connect("amqp://localhost:5672/%2f", ConnectionProperties::default()).await?;
    println!("Connected to RabbitMQ");

    let channel = conn.create_channel().await?;
    println!("Channel created");

    let queue = channel.queue_declare(
        "hello-queue",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    ).await?;
    println!("Declared queue {:?}", queue);

    let mut consumer = channel.basic_consume(
        "hello-queue",
        "my_consumer",
        BasicConsumeOptions::default(),
        FieldTable::default(),
    ).await?;

    println!("Waiting for messages...");

    while let Some(result) = consumer.next().await {
        if let Ok(delivery) = result {
            println!("Received: {:?}", String::from_utf8_lossy(&delivery.data));
            delivery.ack(BasicAckOptions::default()).await?;
        }
    }

    Ok(())
}
