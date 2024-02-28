// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
// use primary::WorkerPrimaryMessage;
use std::collections::{VecDeque, HashMap};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::fs;
use std::net::SocketAddr;
use std::time::{UNIX_EPOCH, SystemTime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

const ONE_SECOND_IN_MILLIS: u128 = 1_000;
// Minimum time the system has to be running to start changing parameters, measured in millis.
const MININUM_RUNNING_TIME: u128 = 500;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    parameter_optimizer: ParameterOptimizer,
}

pub struct InputRate {
    // Queue of previous tranasctions, used to remove old transactions to calculate new rate.
    transaction_queue: VecDeque<(u64, u64)>,
    // Current input rate (transactions / sec).
    transaction_rate: u64,
}

pub struct ParameterOptimizer {
    // Current input rate,
    input_rate: InputRate,
    // Time when the system started.
    first_tx_time: Instant,
    first_tx_recvd: bool,
    // Current system level. Lower levels optimize for latency, higher levels for throughput.
    current_level: usize,
    // Max level the system can have. Currently it is only 1, will be increased in the future.
    batch_sizes: Vec<usize>,
    // // Output channel to inform proposer about changing system level.
    // tx_change_level: Sender<Vec<u8>>,
    // Maps input rate to system level
    config_map: HashMap<u64, usize>,
    // Known input rates sorted in descending order.
    sorted_input_rates: Vec<u64>,
    // Total number of workers
    total_worker_count: usize,
}

impl ParameterOptimizer {
    pub fn new(total_worker_count: usize) -> Self {
        info!("Total worker count: {}", total_worker_count);
        Self {
            input_rate: InputRate::new(),
            first_tx_time: Instant::now(),
            first_tx_recvd: false,
            current_level: 1,
            batch_sizes: vec![1, 1_000, 500_000],
            // tx_change_level,
            config_map: HashMap::new(),
            sorted_input_rates: Vec::new(),
            total_worker_count,
        }
    }

    pub async fn adjust_parameters(&mut self) {
        if self.first_tx_time.elapsed().as_millis() >= MININUM_RUNNING_TIME {
            let current_rate = self.get_current_rate();
            for &input_rate in self.sorted_input_rates.iter() {
                if current_rate < input_rate / self.total_worker_count as u64 {
                    if self.config_map[&input_rate] != self.current_level {
                        info!("At rate {} changing system level to {}", current_rate, self.config_map[&input_rate]);
                        self.change_level(self.config_map[&input_rate]).await;
                    }
                    break;
                }
            }
        }
    }

    fn get_current_rate(&self) -> u64 {
        let elapsed = self.first_tx_time.elapsed().as_millis();
        if elapsed < ONE_SECOND_IN_MILLIS {
            return self.input_rate.transaction_rate / ((elapsed * ONE_SECOND_IN_MILLIS) as u64);
        }
        self.input_rate.transaction_rate
    }

    async fn change_level(&mut self, new_level: usize) {
        self.current_level = new_level;

        // // Change the level of proposer
        // let message = WorkerPrimaryMessage::ChangeLevel(new_level);
        // let message = bincode::serialize(&message)
        //     .expect("Failed to serialize change level message");

        // self.tx_change_level
        //     .send(message)
        //     .await
        //     .expect("Failed to send level change to proposer");
    }

    fn load_config(&mut self) {
        match fs::read_to_string("system_level_config.txt") {
            Ok(data) => {
                for line in data.lines() {
                    let mut split_line = line.split_whitespace();
                    if let (Some(input_rate), Some(level)) = (split_line.next(), split_line.next()) {
                        self.config_map.insert(
                            input_rate.parse::<u64>().unwrap(),
                            level.parse::<usize>().unwrap()
                        );
                    }
                }
            },
            Err(_) => {
                // Default config
                self.config_map.insert(5_000, 0);
                self.config_map.insert(7_000, 1);
                self.config_map.insert(1_000_000, 2);
            }
        };

        self.sorted_input_rates = self.config_map.keys().cloned().collect();
        self.sorted_input_rates.sort();
    }
}

impl InputRate {
    pub fn new() -> Self {
        Self {
            transaction_queue: VecDeque::new(),
            transaction_rate: 0,
        }
    }

    pub fn add_transactions(&mut self, size: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis() as u64;
        self.transaction_queue.push_back((now, size));
        self.transaction_rate += size;

        // remove old measurements
        while self.transaction_queue.len() > 0 && self.transaction_queue.front().unwrap().0 + (ONE_SECOND_IN_MILLIS as u64) < now {
            self.transaction_rate -= self.transaction_queue.pop_front().unwrap().1;
        }
    }
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        // tx_change_level: Sender<Vec<u8>>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        total_worker_count: usize,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                parameter_optimizer: ParameterOptimizer::new(total_worker_count),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        self.parameter_optimizer.load_config();
        self.max_batch_delay = 200;

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if !self.parameter_optimizer.first_tx_recvd {
                        self.parameter_optimizer.first_tx_recvd = true;
                        self.parameter_optimizer.first_tx_time = Instant::now();
                    }
                    if self.current_batch_size >= self.parameter_optimizer.batch_sizes[self.parameter_optimizer.current_level] {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        let transaction_count = self.current_batch.len();
        self.parameter_optimizer
            .input_rate
            .add_transactions(transaction_count as u64);
        self.parameter_optimizer.adjust_parameters().await;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();

        let message = WorkerMessage::Batch(batch, transaction_count);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest::new_with_hash(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                transaction_count,
            })
            .await
            .expect("Failed to deliver batch");
    }
}
