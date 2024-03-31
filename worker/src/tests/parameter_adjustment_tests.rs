use core::panic;
use std::{fs::File, io::{Error}};
use std::io::{Result, Read, Write};
// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{transaction};
use tokio::{sync::mpsc::channel, time::{interval, timeout}};
use tokio::time::sleep;

const PATH: &str = "system_level_config.txt";
const SLEEP_TIME_IN_MILLIS: u64 = 1_100; 
const BATCH_SIZE: usize = 1; 
const BATCH_DELAY: u64 = 100; 
const TOTAL_WORKER_COUNT: usize = 1; 
const CHANNEL_CAPACITY: usize = 1_000;
const CHANNEL_TIMEOUT: u64 = 1_000;

pub fn save_file_content(path: &str) -> Result<String> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

pub fn restore_content(path: &str, content: &str) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

pub fn create_param_config_file(path: &str) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(b"15,10,10,f\n")?;
    file.write_all(b"25,100,100,2f+1")?;
    Ok(())
}

pub async fn assert_header_update(rx_change_header_size: &mut Receiver<Vec<u8>>, expected_header_size: usize) {
    match timeout(Duration::from_millis(CHANNEL_TIMEOUT), rx_change_header_size.recv()).await {
        Ok(Some(new_header_bytes)) => {
            match bincode::deserialize(&new_header_bytes) {
                Ok(WorkerPrimaryMessage::ChangeHeader(header_size)) => {
                    assert_eq!(expected_header_size, header_size);
                },
                _ => panic!("Received wrong header size.")
            }
        },  
        Ok(None) => {},
        Err(_) => panic!("No message received.")
    };
}

#[tokio::test]
async fn receive_header_update() {
    let original_content = save_file_content(&PATH);
    match create_param_config_file(&PATH) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    };

    let (tx_transaction, rx_transaction) = channel(CHANNEL_CAPACITY);
    let (tx_message, _rx_message) = channel(CHANNEL_CAPACITY);
    let (tx_change_header_size, mut rx_change_header_size) = channel(CHANNEL_CAPACITY);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        BATCH_SIZE,
        BATCH_DELAY,
        rx_transaction,
        tx_message,
        tx_change_header_size,
        dummy_addresses,
        TOTAL_WORKER_COUNT,
        false,
    );

    tx_transaction.send(transaction()).await.unwrap();
    sleep(Duration::from_millis(SLEEP_TIME_IN_MILLIS)).await;
    tx_transaction.send(transaction()).await.unwrap();

    let _= match original_content {
        Ok(content) => restore_content(&PATH, &content),
        _  => Ok(()),
    };

    match timeout(Duration::from_millis(CHANNEL_TIMEOUT), rx_change_header_size.recv()).await {
        Ok(Some(new_header_bytes)) => {
            match bincode::deserialize(&new_header_bytes) {
                Ok(WorkerPrimaryMessage::ChangeHeader(header_size)) => {
                    assert_eq!(expected_header_size, header_size);
                },
                _ => panic!("Received wrong header size.")
            }
        },  
        Ok(None) => {},
        Err(_) => panic!("No message received.")
    };
}

#[tokio::test]
async fn receive_no_header_update_when_learning_in_learning_mode() {
    let path = "system_level_config.txt";
    let original_content = save_file_content(&path);
    match create_param_config_file(&path) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    }

    let (tx_transaction, rx_transaction) = channel(CHANNEL_CAPACITY);
    let (tx_message, mut _rx_message) = channel(CHANNEL_CAPACITY);
    let (tx_change_header_size, mut rx_change_header_size) = channel(CHANNEL_CAPACITY);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        BATCH_SIZE,
        BATCH_DELAY,
        rx_transaction,
        tx_message,
        tx_change_header_size,
        dummy_addresses,
        TOTAL_WORKER_COUNT,
        true,
    );

    tx_transaction.send(transaction()).await.unwrap();
    sleep(Duration::from_millis(SLEEP_TIME_IN_MILLIS)).await;
    tx_transaction.send(transaction()).await.unwrap();

    let _= match original_content {
        Ok(content) => restore_content(&path, &content),
        _  => Ok(()),
    };

    match timeout(Duration::from_millis(SLEEP_TIME_IN_MILLIS + 100), rx_change_header_size.recv()).await {
        Ok(Some(_)) => {
            panic!("Was not supposed to receive a header")
        },  
        Ok(None) => {},
        Err(_) => {}
    };
}

#[tokio::test]
async fn parameters_reach_all_levels() {
    let path = "system_level_config.txt";
    let original_content = save_file_content(&path);
    match create_param_config_file(&path) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    }

    let (tx_transaction, rx_transaction) = channel(1000);
    let (tx_message, mut _rx_message) = channel(1000);
    let (tx_change_header_size, mut rx_change_header_size) = channel(1000);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        BATCH_SIZE,
        BATCH_DELAY,
        rx_transaction,
        tx_message,
        tx_change_header_size,
        dummy_addresses,
        TOTAL_WORKER_COUNT,
        false,
    );

    tx_transaction.send(transaction()).await.unwrap();
    sleep(Duration::from_millis(SLEEP_TIME_IN_MILLIS)).await;
    for _ in 1..50 {
        tx_transaction.send(transaction()).await.unwrap();
    }


    let _= match original_content {
        Ok(content) => restore_content(&path, &content),
        _  => Ok(()),
    };

    let mut messages_rcvd = 0;
    loop {
        assert_header_update(&mut rx_change_header_size, 10_usize.pow(messages_rcvd+1)).await;
        messages_rcvd += 1;
        if messages_rcvd >= 3 {
            break;
        }
    }
}

#[tokio::test]
async fn do_not_measure_input_rate_during_first_second() {
    let path = "system_level_config.txt";
    let original_content = save_file_content(&path);
    match create_param_config_file(&path) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    }

    let (tx_transaction, rx_transaction) = channel(CHANNEL_CAPACITY);
    let (tx_message, mut _rx_message) = channel(CHANNEL_CAPACITY);
    let (tx_change_header_size, mut rx_change_header_size) = channel(CHANNEL_CAPACITY);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        BATCH_SIZE,
        BATCH_DELAY,
        rx_transaction,
        tx_message,
        tx_change_header_size,
        dummy_addresses,
        TOTAL_WORKER_COUNT,
        false,
    );

    tx_transaction.send(transaction()).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    tx_transaction.send(transaction()).await.unwrap();

    let _= match original_content {
        Ok(content) => restore_content(&path, &content),
        _  => Ok(()),
    };

    match timeout(Duration::from_millis(SLEEP_TIME_IN_MILLIS + 100), rx_change_header_size.recv()).await {
        Ok(Some(_)) => {
            panic!("Was not supposed to receive a header")
        },  
        Ok(None) => {},
        Err(_) => {}
    };
}

#[tokio::test]
async fn detect_decreasing_input_load() {
    let path = "system_level_config.txt";
    let original_content = save_file_content(&path);
    match create_param_config_file(&path) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    }

    let (tx_transaction, rx_transaction) = channel(CHANNEL_CAPACITY);
    let (tx_message, mut _rx_message) = channel(CHANNEL_CAPACITY);
    let (tx_change_header_size, mut rx_change_header_size) = channel(CHANNEL_CAPACITY);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        BATCH_SIZE,
        BATCH_DELAY,
        rx_transaction,
        tx_message,
        tx_change_header_size,
        dummy_addresses,
        TOTAL_WORKER_COUNT,
        false,
    );

    let _= match original_content {
        Ok(content) => restore_content(&path, &content),
        _  => Ok(()),
    };

    tx_transaction.send(transaction()).await.unwrap();
    sleep(Duration::from_millis(600)).await;

    for _ in 1..21 {
        tx_transaction.send(transaction()).await.unwrap();
    }
    sleep(Duration::from_millis(600)).await;
    tx_transaction.send(transaction()).await.unwrap();

    assert_header_update(&mut rx_change_header_size, 100).await;

    sleep(Duration::from_millis(600)).await;
    tx_transaction.send(transaction()).await.unwrap();    
    
    assert_header_update(&mut rx_change_header_size, 10).await;
}

#[test]
fn load_correct_param_config() {
    let (tx_change_header_size, _rx_change_header_size) = channel(CHANNEL_CAPACITY);
    
    let mut parameter_optimizer = ParameterOptimizer::new(tx_change_header_size, TOTAL_WORKER_COUNT);
    let original_content = save_file_content(&PATH);

    match create_param_config_file(&PATH) {
        Ok(()) => {},
        _ => panic!("Failed to create param file")
    }

    parameter_optimizer.load_config();

    let _= match original_content {
        Ok(content) => restore_content(&PATH, &content),
        _  => Ok(()),
    };


    assert_eq!(parameter_optimizer.config_map.len(), 2);
    assert_eq!(*parameter_optimizer.config_map.get(&15).unwrap(), Parameters::new(15, 10, 10, "f".to_owned()));
    assert_eq!(*parameter_optimizer.config_map.get(&25).unwrap(), Parameters::new(25, 100, 100, "2f+1".to_owned()));
}

#[test]
fn loads_default_params_when_file_does_not_exist() {
    let (tx_change_header_size, _rx_change_header_size) = channel(CHANNEL_CAPACITY);
    
    let mut parameter_optimizer = ParameterOptimizer::new(tx_change_header_size, TOTAL_WORKER_COUNT);
    let original_content = save_file_content(&PATH);

    match fs::remove_file(&PATH) {
        Ok(_) => {},
        Err(_) => {}
    };

    parameter_optimizer.load_config();

    let _= match original_content {
        Ok(content) => restore_content(&PATH, &content),
        _  => Ok(()),
    };

    assert_eq!(parameter_optimizer.config_map.len(), 3);
    assert_eq!(*parameter_optimizer.config_map.get(&3_000).unwrap(), Parameters::new(3_000, 1, 1, "f".to_owned()));
    assert_eq!(*parameter_optimizer.config_map.get(&8_000).unwrap(), Parameters::new(8_000, 1, 1, "f+1".to_owned()));
    assert_eq!(*parameter_optimizer.config_map.get(&1_000_000).unwrap(), Parameters::new(1_000_000, 500_000, 1_000, "2f+1".to_owned()));
}

#[test]
fn adds_transactions_to_queue() {
    let (tx_change_header_size, _rx_change_header_size) = channel(CHANNEL_CAPACITY);
    
    let mut parameter_optimizer = ParameterOptimizer::new(tx_change_header_size, TOTAL_WORKER_COUNT);


    assert_eq!(parameter_optimizer.get_current_rate(), 0);
    parameter_optimizer.add_transactions(1);
    assert_eq!(parameter_optimizer.get_current_rate(), 1);
    parameter_optimizer.add_transactions(100);
    assert_eq!(parameter_optimizer.get_current_rate(), 101);
}

#[tokio::test]
async fn removes_old_transactions() {
    let (tx_change_header_size, _rx_change_header_size) = channel(CHANNEL_CAPACITY);
    
    let mut parameter_optimizer = ParameterOptimizer::new(tx_change_header_size, TOTAL_WORKER_COUNT);

    parameter_optimizer.add_transactions(1);
    assert_eq!(parameter_optimizer.get_current_rate(), 1);
    sleep(Duration::from_millis(1_100)).await;
    parameter_optimizer.add_transactions(2);
    assert_eq!(parameter_optimizer.get_current_rate(), 2);
}

#[tokio::test]
async fn measures_rate_before_1_second() {
    let (tx_change_header_size, _rx_change_header_size) = channel(CHANNEL_CAPACITY);
    
    let mut parameter_optimizer = ParameterOptimizer::new(tx_change_header_size, TOTAL_WORKER_COUNT);

    parameter_optimizer.add_transactions(100);
    sleep(Duration::from_millis(600)).await;
    if parameter_optimizer.get_current_rate() <= 100 || parameter_optimizer.get_current_rate() >= 200 {
        panic!("Wrong input rate measurement.");
    }
}