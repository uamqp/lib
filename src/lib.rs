use tokio::runtime::Runtime;

// use futures::executor::block_on;

use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Consumer};

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
    callback: extern "C" fn(Box<Arc<Consumer>>) -> (),
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
            .await.unwrap();

        callback(Box::new(Arc::new(consumer)));
    });
}
