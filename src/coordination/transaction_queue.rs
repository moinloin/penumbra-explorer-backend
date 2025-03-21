use cometindex::ContextualizedEvent;
use sqlx::types::chrono::DateTime;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Retry information for transactions that failed to process
#[derive(Debug, Clone)]
pub struct RetryInfo {
    pub attempts: u32,
    pub next_retry: Instant,
}

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
    pub retry_info: Option<RetryInfo>,
}

/// A thread-safe queue for coordinating transaction processing between app views
#[derive(Debug, Default)]
pub struct TransactionQueue {
    pending_batches: VecDeque<TransactionBatch>,
    failed_blocks: HashMap<u64, RetryInfo>,
}

impl TransactionQueue {
    /// Create a new empty transaction queue
    pub fn new() -> Self {
        Self {
            pending_batches: VecDeque::new(),
            failed_blocks: HashMap::new(),
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

    /// Take all pending transaction batches that are ready to process
    pub fn take_ready_batches(&mut self) -> Vec<TransactionBatch> {
        let now = Instant::now();
        let mut ready_batches = Vec::new();
        let mut not_ready = VecDeque::new();

        self.failed_blocks
            .retain(|_, retry_info| retry_info.next_retry > now);

        while let Some(batch) = self.pending_batches.pop_front() {
            if let Some(retry_info) = self.failed_blocks.get(&batch.block_height) {
                if retry_info.next_retry > now {
                    not_ready.push_back(batch);
                    continue;
                }
            }

            let all_ready = batch
                .transactions
                .iter()
                .all(|tx| match tx.retry_info.as_ref() {
                    None => true,
                    Some(r) => r.next_retry <= now,
                });
            if all_ready {
                ready_batches.push(batch);
            } else {
                not_ready.push_back(batch);
            }
        }

        self.pending_batches = not_ready;
        ready_batches
    }

    /// Mark a block as having failed processing (usually due to FK constraint)
    pub fn mark_block_failed(&mut self, block_height: u64) {
        let attempts = self
            .failed_blocks
            .get(&block_height)
            .map_or(1, |retry| retry.attempts + 1);

        // Exponential backoff with a cap
        let delay = Duration::from_millis(500) * (1 << attempts.min(8));

        self.failed_blocks.insert(
            block_height,
            RetryInfo {
                attempts,
                next_retry: Instant::now() + delay,
            },
        );
    }

    /// Requeue a batch of transactions with one transaction marked for retry
    pub fn requeue_batch_with_retries(
        &mut self,
        mut batch: TransactionBatch,
        failed_tx_hashes: &[([u8; 32], String)],
    ) {
        let now = Instant::now();

        let tx_hash_set: std::collections::HashSet<_> =
            failed_tx_hashes.iter().map(|(hash, _)| hash).collect();

        for tx in &mut batch.transactions {
            if tx_hash_set.contains(&tx.tx_hash) {
                let attempts = tx.retry_info.as_ref().map_or(1, |r| r.attempts + 1);
                let delay = Duration::from_secs(2_u64.pow(attempts.min(6)));

                tx.retry_info = Some(RetryInfo {
                    attempts,
                    next_retry: now + delay,
                });
            }
        }

        self.enqueue_batch(batch);
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
