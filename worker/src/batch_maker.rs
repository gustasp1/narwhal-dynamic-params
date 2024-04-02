// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
use log::info;
use network::ReliableSender;
use primary::WorkerPrimaryMessage;
use std::collections::{BTreeMap, VecDeque};
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

#[cfg(test)]
#[path = "tests/parameter_adjustment_tests.rs"]
pub mod parameter_adjustment_tests;

const ONE_SECOND_IN_MILLIS: u64 = 1_000;
const MININUM_RUNNING_TIME: u128 = 1_000;

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
        /// If the learning flag is set, do not change parameters based on input load because we 
    /// are trying to test the latency for a particular input rate and system level
    learning: bool,
}

pub struct InputRate {
    
}

#[derive(Clone)]
#[derive(PartialEq)]
#[derive(Debug)]
pub struct Parameters {
    input_rate: usize,
    batch_size: usize,
    header_size: usize,
    quorum_threshold: String,
}


impl Parameters {
    pub fn new(input_rate: usize, batch_size: usize, header_size: usize, quorum_threshold: String) -> Self {
        Self {
            input_rate,
            batch_size,
            header_size,
            quorum_threshold,
        }
    }

    pub fn default() -> Self {
        Self {
            input_rate: 0,
            batch_size: 500_000,
            header_size: 1_000,
            quorum_threshold: "2f+1".to_owned(),
        }
    }
}

pub struct ParameterOptimizer {
    // Time when the system started.
    first_tx_time: Instant,
    // // Output channel to inform proposer about changing system level.
    tx_change_header_size: Sender<Vec<u8>>,
    // Maps input rate to system level
    config_map: BTreeMap<usize, Parameters>,
    // Total number of workers
    total_worker_count: usize,
    current_params: Parameters,
    // Queue of previous tranasctions, used to remove old transactions to calculate new rate.
    transaction_queue: VecDeque<(u64, u64)>,
    // Current input rate (transactions / sec).
    transaction_rate: u64,
}

impl ParameterOptimizer {
    pub fn new(tx_change_header_size: Sender<Vec<u8>>, total_worker_count: usize) -> Self {
        info!("Total worker count: {}", total_worker_count);
        Self {
            first_tx_time: Instant::now(),
            tx_change_header_size,
            config_map: BTreeMap::new(),
            total_worker_count,
            current_params: Parameters::new(1_000_001, 500_000, 1_000, "2f+1".to_owned()),
            transaction_queue: VecDeque::new(),
            transaction_rate: 0,
        }
    }

    pub async fn adjust_parameters(&mut self, batch_size: &mut usize) {
        if self.first_tx_time.elapsed().as_millis() >= MININUM_RUNNING_TIME {
            let current_rate = self.get_current_rate() * self.total_worker_count;
            // Find the parameters associated with the first input rate that's larger
            // than the current rate
            let entry = self
                .config_map
                .range(current_rate..)
                .next()
                .map(|(k, v)| (*k, v.clone()))
                .unwrap_or_else(|| (0, Parameters::default()));
            let next_input_rate = entry.0;
            if next_input_rate != self.current_params.input_rate {
                let params = entry.1;
                info!("At rate {}, changing params to {} {} {}", current_rate, params.batch_size, params.header_size, params.quorum_threshold);
                self.current_params = params.clone();
                *batch_size = params.batch_size;
                self.inform_proposer(params.header_size).await;
            }
        }
    }

    fn get_current_rate(&self) -> usize {
        let elapsed = self.first_tx_time.elapsed().as_millis() as u64;
        if elapsed < ONE_SECOND_IN_MILLIS && elapsed > 0 {
            return (self.transaction_rate * ONE_SECOND_IN_MILLIS / elapsed) as usize;
        }
        self.transaction_rate as usize
    }

    async fn inform_proposer(&self, header_size: usize) {
        // Change the level of proposer
        let message = WorkerPrimaryMessage::ChangeHeader(header_size);
        let message = bincode::serialize(&message)
            .expect("Failed to serialize change level message");

        self.tx_change_header_size
            .send(message)
            .await
            .expect("Failed to send level change to proposer");
    }

    fn load_config(&mut self) {
        match fs::read_to_string("system_level_config.txt") {
            Ok(data) => {
                for line in data.lines() {
                    let parts: Vec<&str> = line.split(",").collect();
                    assert!(parts.len() == 4, "Parameter config has to contain 4 fields.");
                    let input_rate = parts[0].parse::<usize>().unwrap();
                    let batch_size = parts[1].parse::<usize>().unwrap();
                    let header_size = parts[2].parse::<usize>().unwrap();
                    let quorum_threshold = parts[3].to_owned();
                    self.config_map.insert(input_rate, Parameters::new(input_rate, batch_size, header_size, quorum_threshold));
                }
            },
            Err(_) => {
                // Default config
                self.config_map.insert(3_000, Parameters::new(3_000, 1, 1, "f".to_owned()));
                self.config_map.insert(8_000, Parameters::new( 8_000, 1, 1, "f+1".to_owned()));
                self.config_map.insert(1_000_000, Parameters::new(1_000_000, 500_000, 1_000, "2f+1".to_owned()));
            }
        };
    }

    pub fn add_transactions(&mut self, size: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis() as u64;
        self.transaction_queue.push_back((now, size));
        self.transaction_rate += size;

        // remove old measurements
        while self.transaction_queue.len() > 0 && self.transaction_queue.front().unwrap().0 + ONE_SECOND_IN_MILLIS < now {
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
        tx_change_header_size: Sender<Vec<u8>>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        total_worker_count: usize,
        learning: bool,
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
                parameter_optimizer: ParameterOptimizer::new(tx_change_header_size, total_worker_count),
                learning,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        info!("Yo got batch size: {} {}", self.batch_size, self.learning);
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        self.parameter_optimizer.load_config();
        self.max_batch_delay = 200;

        if let Some(transaction) = self.rx_transaction.recv().await {
            self.current_batch_size += transaction.len();
            self.current_batch.push(transaction);
            
            self.parameter_optimizer.first_tx_time = Instant::now();
        }

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);

                    if self.current_batch_size >= self.batch_size {
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
        if !self.learning {
            self.parameter_optimizer
                .add_transactions(transaction_count as u64);
            self.parameter_optimizer.adjust_parameters(&mut self.batch_size).await;
        }

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