use cometindex::ContextualizedEvent;
use sqlx::types::chrono::DateTime;
use std::collections::VecDeque;

/// Represents a batch of transactions from a single block
#[derive(Debug, Clone)]
pub struct TransactionBatch {
    pub block_height: u64,
    pub timestamp: DateTime<sqlx::types::chrono::Utc>,
    pub transactions: Vec<PendingTransaction>,
}

/// A transaction that is waiting to be processed
#[derive(Debug, Clone)]
pub struct PendingTransaction {
    pub tx_hash: [u8; 32],
    pub tx_bytes: Vec<u8>,
    pub tx_index: u64,
    pub events: Vec<ContextualizedEvent<'static>>,
}

/// A thread-safe queue for coordinating transaction processing between app views
#[derive(Debug, Default)]
pub struct TransactionQueue {
    pending_batches: VecDeque<TransactionBatch>,
}

impl TransactionQueue {
    /// Create a new empty transaction queue
    pub fn new() -> Self {
        Self {
            pending_batches: VecDeque::new(),
        }
    }

    /// Add a new batch of transactions from a block
    pub fn enqueue_batch(&mut self, batch: TransactionBatch) {
        self.pending_batches.push_back(batch);
    }

    /// Create and add a new transaction batch
    pub fn create_batch(
        &mut self,
        block_height: u64,
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        transactions: Vec<PendingTransaction>,
    ) {
        if !transactions.is_empty() {
            let batch = TransactionBatch {
                block_height,
                timestamp,
                transactions,
            };
            self.enqueue_batch(batch);
        }
    }

    /// Take all pending transaction batches, leaving the queue empty
    pub fn take_all_batches(&mut self) -> Vec<TransactionBatch> {
        let mut batches = Vec::with_capacity(self.pending_batches.len());
        while let Some(batch) = self.pending_batches.pop_front() {
            batches.push(batch);
        }
        batches
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.pending_batches.is_empty()
    }

    /// Get the number of batches in the queue
    pub fn len(&self) -> usize {
        self.pending_batches.len()
    }

    /// Get the total number of transactions across all batches
    pub fn transaction_count(&self) -> usize {
        self.pending_batches
            .iter()
            .map(|batch| batch.transactions.len())
            .sum()
    }
}
