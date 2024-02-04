// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::{SerializedBatchMessage, ProcessorMessage};
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
    /// The number of transactions in the batch.
    pub transaction_count: usize,
    /// Mean start time of first and last batch transactions.
    pub mean_start_time: u64,
    /// Current system level.
    pub level: usize,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<ProcessorMessage>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<ProcessorMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers, transaction_count, mean_start_time, level}) = self.rx_message.recv().await {

            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if total_stake >= self.committee.quorum_waiter_threshold(level) {
                    self.tx_batch
                        .send(ProcessorMessage { batch, transaction_count, mean_start_time})
                        .await
                        .expect("Failed to deliver batch");
                    break;
                }
            }
        }
    }
}