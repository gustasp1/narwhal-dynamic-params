// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use network::SimpleSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,

    rx_change_header_size: Receiver<Vec<u8>>,
}

impl PrimaryConnector {
    pub fn spawn(
        primary_address: SocketAddr,
        rx_digest: Receiver<SerializedBatchDigestMessage>,
        rx_change_header_size: Receiver<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new(),
                rx_change_header_size,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_digest.recv() => {
                    // Send the digest through the network.
                    self.network
                        .send(self.primary_address, Bytes::from(digest))
                        .await;
                },
                Some(level) = self.rx_change_header_size.recv() => {
                    self.network
                        .send(self.primary_address, Bytes::from(level))
                        .await;
                }
            }
        }
    }
}
