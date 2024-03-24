// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Receives level change from our workers.
    rx_change_level: Receiver<usize>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// Header sizes and delays, set based on the current system level
    param_config: ProposerParameterConfig,
}

pub struct ProposerParameterConfig {
    /// Header sizes for different system levels.
    header_sizes: Vec<usize>,
    /// Header delays for different system levels.
    header_delays: Vec<u64>,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_change_level: Receiver<usize>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();
        info!("proposer header size {}", header_size);

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                rx_change_level,
                tx_core,
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                param_config: ProposerParameterConfig {
                    header_sizes: vec![1, 1, 1_000],
                    header_delays: vec![200, 200, 200],
                },
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        info!("Yo got header size: {}", self.header_size);

        //self.header_size = self.param_config.header_sizes[0];
        self.max_header_delay = 200;

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                },
                Some(level) = self.rx_change_level.recv() => {
                    info!("received level {}", level);
                    self.header_size = self.param_config.header_sizes[level];
                    self.max_header_delay = self.param_config.header_delays[level];
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
