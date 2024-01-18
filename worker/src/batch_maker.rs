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
use primary::WorkerPrimaryMessage;
use std::collections::VecDeque;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::time::{UNIX_EPOCH, SystemTime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use byteorder::{BigEndian, ByteOrder};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

const ONE_SECOND_IN_MILLIS: u128 = 1_000;
// Minimum time the system has to be running to start changing parameters, measured in millis.
const MININUM_RUNNING_TIME: u128 = 100;
// When checking if we need to decrease current system level, we do not want to decrease level as soon
// current input rate is lower than current threshold because if the input load is close to a certain threshold,
// the level will be increasing and decreasing very frequently. For example, if the current threshold is 10_000,
// the input rate has to be lower that 10_000 * THRESHOLD_FACTOR for the system level to go back to previous state.
const THRESHOLD_FACTOR: f64 = 0.9;

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
    transaction_queue: VecDeque<(u128, u64)>,
    // Current input rate (transactions / sec).
    transaction_rate: u64,
}

pub struct ParameterOptimizer {
    // Current input rate.
    input_rate: InputRate,
    // Time when the system started.
    system_start_time: u128,
    // Current system level. Lower levels optimize for latency, higher levels for throughput.
    current_level: usize,
    // Max level the system can have. Currently it is only 1, will be increased in the future.
    max_level: usize,
    // Batch size for each system level.
    batch_sizes: Vec<usize>,
    // Input rate threshold at which to increase system level (if it is not the max level).
    transaction_rate_thresholds: Vec<usize>,
    // Output channel to inform proposer about changing system level.
    tx_change_level: Sender<Vec<u8>>,
}

impl ParameterOptimizer {
    pub fn new(tx_change_level: Sender<Vec<u8>>, total_worker_count: usize) -> Self {
        info!("total worker count: {}", total_worker_count);
        Self {
            input_rate: InputRate::new(),
            system_start_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to measure time")
                    .as_millis(),
            current_level: 0,
            max_level: 2,
            batch_sizes: vec![1, 2_000, 600_000],
            transaction_rate_thresholds: vec![10_000, 40_000, 0]
                .iter()
                .map(|&size| size / total_worker_count)
                .collect(),
            tx_change_level
        }
    }

    pub async fn adjust_parameters(&mut self, batch_size: &mut usize) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();
        if self.system_start_time + MININUM_RUNNING_TIME < now {
            let current_rate = self.get_current_rate();
            // Check if we need to increase
            if current_rate > self.transaction_rate_thresholds[self.current_level] && self.current_level < self.max_level {
                info!("Increasing system level to: {}", self.current_level + 1);
                self.current_level += 1;
                *batch_size = self.batch_sizes[self.current_level];

                self.change_proposer_level(self.current_level).await;
            }
            // Check if we need to decrease
            if self.current_level > 0 && (current_rate as f64) < self.transaction_rate_thresholds[self.current_level-1] as f64 * THRESHOLD_FACTOR {
                info!("Decreasing system level to: {}", self.current_level - 1);
                self.current_level -= 1;
                *batch_size = self.batch_sizes[self.current_level];

                self.change_proposer_level(self.current_level).await;
            }
        }
    }

    fn get_current_rate(&self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis() ;
        let diff = now - self.system_start_time;
        if diff < ONE_SECOND_IN_MILLIS {
            return (self.input_rate.transaction_rate as u128 / diff * ONE_SECOND_IN_MILLIS) as usize;
        }
        self.input_rate.transaction_rate as usize
    }

    async fn change_proposer_level(&mut self, new_level: usize) {
        // Increase the level of proposerq
        let message = WorkerPrimaryMessage::ChangeLevel(new_level);
        let message = bincode::serialize(&message)
            .expect("Failed to serialize change level message");

        self.tx_change_level
            .send(message)
            .await
            .expect("Failed to send level change to proposer");
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
            .as_millis();
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
        tx_change_level: Sender<Vec<u8>>,
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
                parameter_optimizer: ParameterOptimizer::new(tx_change_level, total_worker_count),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        self.batch_size = self.parameter_optimizer.batch_sizes[0];
        info!("batch size: {}", self.batch_size);

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
        self.parameter_optimizer
           .input_rate
           .add_transactions(transaction_count as u64);
        self.parameter_optimizer.adjust_parameters(&mut self.batch_size).await;

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

        let mut mean_start_time = 0;
        let transaction_count = batch.len();

        if let (Some(first_transaction), Some(last_transaction)) = (batch.first(), batch.last()) {
            let first_transaction_timestamp = BigEndian::read_u64(&first_transaction[9..17]);
            let last_transaction_timestamp = BigEndian::read_u64(&last_transaction[9..17]);
            mean_start_time = (first_transaction_timestamp + last_transaction_timestamp) / 2;
        }


        let message = WorkerMessage::Batch(batch, transaction_count, mean_start_time);
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
                mean_start_time,
            })
            .await
            .expect("Failed to deliver batch");
    }
}
