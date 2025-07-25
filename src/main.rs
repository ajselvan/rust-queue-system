use warp::Filter;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;

#[derive(Debug, Deserialize, Serialize)]
struct AddressInput {
    address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Order {
    address: String,
    market_id: String,
    leverage: u32,
    order_type: u8,
    timestamp: u64,
    price: String,
    total_size: String,
    remaining_size: String,
    order_value: String,
    order_id: String,
    trade_id: String,
    last_updated: u64,
    transaction_version: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let route = warp::post()
        .and(warp::path("emit_order"))
        .and(warp::body::json())
        .and_then(handle_emit_order);

    println!("Rust API running at http://localhost:3030");
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}

async fn handle_emit_order(input: AddressInput) -> Result<impl warp::Reply, Infallible> {
    match fetch_and_filter_orders(&input.address).await {
        Ok(filtered_orders) if !filtered_orders.is_empty() => {
            let payload = serde_json::to_string(&filtered_orders).unwrap();

            if let Err(e) = publish_to_rabbitmq(&input.address, payload.clone()).await {
                eprintln!("❌ Failed to publish: {:?}", e);
                return Ok(warp::reply::with_status(
                    "Failed to publish",
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }

            println!("✅ Published orders for {}: {}", input.address, payload);
            Ok(warp::reply::with_status("Order emitted", warp::http::StatusCode::OK))
        }
        Ok(_) => {
            println!("⚠️ No orders found for address: {}", input.address);
            Ok(warp::reply::with_status(
                "No matching orders found",
                warp::http::StatusCode::NOT_FOUND,
            ))
        }
        Err(err) => {
            eprintln!("❌ Error fetching orders: {:?}", err);
            Ok(warp::reply::with_status(
                "Error fetching orders",
                warp::http::StatusCode::BAD_GATEWAY,
            ))
        }
    }
}

async fn fetch_and_filter_orders(address: &str) -> Result<Vec<Order>, reqwest::Error> {
    let response = reqwest::get("https://perpetuals-indexer.kana.trade/open_orders")
        .await?
        .json::<Vec<Order>>()
        .await?;

    let filtered_orders = response
        .into_iter()
        .filter(|order| order.address.eq_ignore_ascii_case(address))
        .collect();

    Ok(filtered_orders)
}

async fn publish_to_rabbitmq(address: &str, payload: String) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::connect("amqp://localhost:5672/%2f", ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let queue_name = format!("order_queue_{}", address.to_lowercase());

    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .basic_publish(
            "",
            &queue_name,
            BasicPublishOptions::default(),
            payload.as_bytes(),
            BasicProperties::default(),
        )
        .await?
        .await?;

    Ok(())
}
