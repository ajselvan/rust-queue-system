use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
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

    let payload = b"Hello from Rust!";
    channel
        .basic_publish(
            "",
            "hello-queue",
            BasicPublishOptions::default(),
            payload,  // ✅ Correct: slice, not Vec
            BasicProperties::default(),
        )
        .await?
        .await?;
    println!("Sent: {}", String::from_utf8_lossy(payload));

    Ok(())
}
