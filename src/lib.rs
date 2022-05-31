use lapin::BasicProperties;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// use futures::executor::block_on;
use futures_lite::stream::StreamExt;
use lapin::{
    message::{Delivery, DeliveryResult},
    options::*,
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer,
};

use std::sync::Arc;

// #[no_mangle]
// pub extern fn connect() -> Box<Connection> {
//     println!("Hello from add_numbers!");
// }

#[no_mangle]
pub extern "C" fn create_async_loop() -> Box<Runtime> {
    println!("Rust: create_async_loop!");
    Box::new(Runtime::new().unwrap())
}

#[no_mangle]
pub extern "C" fn connect(rnt: &Runtime, callback: extern "C" fn(Box<Arc<Connection>>) -> ()) {
    println!("Rust: connect!");
    let addr = "amqps://rabbitmq:rabbitmq@localhost:5671/%2f";

    rnt.spawn(async move {
        println!("Rust: async connect!");
        let conn = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .unwrap();

        println!("Rust: Connected!");
        callback(Box::new(Arc::new(conn)));
    });
}

#[no_mangle]
pub extern "C" fn create_channel(
    rnt: &Runtime,
    conn: &Arc<Connection>,
    callback: extern "C" fn(Box<Arc<Channel>>) -> (),
) {
    println!("Rust: create_channel!");

    let conn_clone = conn.clone();

    rnt.spawn(async move {
        println!("Rust: async create_channel!");
        let channel = conn_clone.create_channel().await.unwrap();
        callback(Box::new(Arc::new(channel)));
    });
}

#[no_mangle]
pub extern "C" fn queue_declare(
    rnt: &Runtime,
    channel: &Arc<Channel>,
    callback: extern "C" fn() -> (),
) {
    println!("Rust: queue_declare!");

    let channel_clone = channel.clone();

    rnt.spawn(async move {
        println!("Rust: async queue_declare!");
        channel_clone
            .queue_declare(
                "hello",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        callback();
    });
}

#[no_mangle]
pub extern "C" fn basic_consume(
    rnt: &Runtime,
    channel: &Arc<Channel>,
    callback: extern "C" fn(Box<Arc<Mutex<Consumer>>>) -> (),
) {
    println!("Rust: basic_consume!");

    let channel_clone = channel.clone();

    rnt.spawn(async move {
        println!("Rust: async basic_consume!");
        let consumer = channel_clone
            .basic_consume(
                "hello",
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        callback(Box::new(Arc::new(Mutex::new(consumer))));
    });
}

#[no_mangle]
pub extern "C" fn basic_publish(
    rnt: &Runtime,
    channel: &Arc<Channel>,
    callback: extern "C" fn() -> (),
) {
    println!("Rust: basic_publish!");

    let channel_clone = channel.clone();

    rnt.spawn(async move {
        println!("Rust: async basic_publish!");

        channel_clone
            .basic_publish(
                "",
                "hello",
                BasicPublishOptions::default(),
                b"Hello world!",
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        callback();
    });
}

#[no_mangle]
pub extern "C" fn next(
    rnt: &Runtime,
    consumer: &Arc<Mutex<Consumer>>,
    callback: extern "C" fn(Box<Arc<Delivery>>) -> (),
) {
    println!("Rust: next!");

    let consumer_clone = consumer.clone();

    rnt.spawn(async move {
        println!("Rust: async next!");
        let delivery = consumer_clone.lock().await.next().await.unwrap().unwrap();

        callback(Box::new(Arc::new(delivery)));
    });
}
