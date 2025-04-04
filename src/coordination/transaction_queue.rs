use cometindex::ContextualizedEvent;
use sqlx::types::chrono::DateTime;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RetryInfo {
    pub attempts: u32,
    pub next_retry: Instant,
}

#[derive(Debug, Clone)]
pub struct TransactionBatch {
    pub block_height: u64,
    pub timestamp: DateTime<sqlx::types::chrono::Utc>,
    pub transactions: Vec<PendingTransaction>,
}

impl TransactionBatch {
    #[must_use]
    pub fn new(
        block_height: u64,
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        transactions: Vec<PendingTransaction>,
    ) -> Self {
        Self {
            block_height,
            timestamp,
            transactions,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PendingTransaction {
    pub tx_hash: [u8; 32],
    pub tx_bytes: Vec<u8>,
    pub tx_index: u64,
    pub events: Vec<ContextualizedEvent<'static>>,
    pub retry_info: Option<RetryInfo>,
}

impl PendingTransaction {
    #[must_use]
    pub fn new(
        tx_hash: [u8; 32],
        tx_bytes: Vec<u8>,
        tx_index: u64,
        events: Vec<ContextualizedEvent<'static>>,
    ) -> Self {
        Self {
            tx_hash,
            tx_bytes,
            tx_index,
            events,
            retry_info: None,
        }
    }

    #[must_use]
    pub fn is_ready(&self, now: Instant) -> bool {
        match &self.retry_info {
            None => true,
            Some(r) => r.next_retry <= now,
        }
    }

    pub fn mark_for_retry(&mut self, error: &str) {
        let now = Instant::now();
        let attempts = self.retry_info.as_ref().map_or(1, |r| r.attempts + 1);
        let delay = Duration::from_secs(2_u64.pow(attempts.min(6)));

        self.retry_info = Some(RetryInfo {
            attempts,
            next_retry: now + delay,
        });

        tracing::debug!(
            "Transaction marked for retry: attempt={}, delay={:?}, error={}",
            attempts,
            delay,
            error
        );
    }
}

#[derive(Debug, Default)]
pub struct TransactionQueue {
    pending_batches: VecDeque<TransactionBatch>,
    failed_blocks: HashMap<u64, RetryInfo>,
}

impl TransactionQueue {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending_batches: VecDeque::new(),
            failed_blocks: HashMap::new(),
        }
    }

    pub fn enqueue_batch(&mut self, batch: TransactionBatch) {
        if !batch.is_empty() {
            tracing::debug!(
                "Enqueuing batch with {} transactions from block {}",
                batch.len(),
                batch.block_height
            );
            self.pending_batches.push_back(batch);
        }
    }

    pub fn create_batch(
        &mut self,
        block_height: u64,
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        transactions: Vec<PendingTransaction>,
    ) {
        if !transactions.is_empty() {
            let batch = TransactionBatch::new(block_height, timestamp, transactions);
            self.enqueue_batch(batch);
        }
    }

    pub fn take_ready_batches(&mut self) -> Vec<TransactionBatch> {
        let now = Instant::now();
        let mut ready_batches = Vec::new();
        let mut not_ready = VecDeque::new();

        self.failed_blocks.retain(|block_height, retry_info| {
            let still_failed = retry_info.next_retry > now;
            if !still_failed {
                tracing::debug!("Block {} retry period expired", block_height);
            }
            still_failed
        });

        while let Some(batch) = self.pending_batches.pop_front() {
            if let Some(retry_info) = self.failed_blocks.get(&batch.block_height) {
                if retry_info.next_retry > now {
                    tracing::debug!(
                        "Block {} not ready yet, retrying in {:?}",
                        batch.block_height,
                        retry_info.next_retry.duration_since(now)
                    );
                    not_ready.push_back(batch);
                    continue;
                }
            }

            let all_ready = batch.transactions.iter().all(|tx| tx.is_ready(now));

            if all_ready {
                tracing::debug!(
                    "Batch for block {} with {} transactions is ready for processing",
                    batch.block_height,
                    batch.len()
                );
                ready_batches.push(batch);
            } else {
                tracing::debug!(
                    "Batch for block {} has transactions not ready for retry",
                    batch.block_height
                );
                not_ready.push_back(batch);
            }
        }

        self.pending_batches = not_ready;
        ready_batches
    }

    pub fn mark_block_failed(&mut self, block_height: u64) {
        let attempts = self
            .failed_blocks
            .get(&block_height)
            .map_or(1, |retry| retry.attempts + 1);

        let delay = Duration::from_millis(500) * (1 << attempts.min(8));
        let next_retry = Instant::now() + delay;

        tracing::info!(
            "Block {} marked as failed, attempt={}, retry in {:?}",
            block_height,
            attempts,
            delay
        );

        self.failed_blocks.insert(
            block_height,
            RetryInfo {
                attempts,
                next_retry,
            },
        );
    }

    pub fn requeue_batch_with_retries(
        &mut self,
        mut batch: TransactionBatch,
        failed_tx_hashes: &[([u8; 32], String)],
    ) {
        if failed_tx_hashes.is_empty() {
            tracing::debug!(
                "Requeuing batch for block {} without any specific transaction failures",
                batch.block_height
            );
            self.enqueue_batch(batch);
            return;
        }

        let tx_hash_set: HashSet<_> = failed_tx_hashes.iter().map(|(hash, _)| hash).collect();
        let error_map: HashMap<_, _> = failed_tx_hashes
            .iter()
            .map(|(hash, error)| (*hash, error.clone()))
            .collect();

        let mut retry_count = 0;

        for tx in &mut batch.transactions {
            if tx_hash_set.contains(&tx.tx_hash) {
                if let Some(error) = error_map.get(&tx.tx_hash).cloned() {
                    tx.mark_for_retry(&error);
                    retry_count += 1;
                }
            }
        }

        tracing::info!(
            "Requeuing batch for block {} with {}/{} transactions marked for retry",
            batch.block_height,
            retry_count,
            batch.len()
        );

        self.enqueue_batch(batch);
    }

    pub fn take_all_batches(&mut self) -> Vec<TransactionBatch> {
        let batches = self.pending_batches.drain(..).collect::<Vec<_>>();
        tracing::debug!("Taking all {} batches from queue", batches.len());
        batches
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending_batches.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pending_batches.len()
    }

    #[must_use]
    pub fn transaction_count(&self) -> usize {
        self.pending_batches.iter().map(TransactionBatch::len).sum()
    }

    #[must_use]
    pub fn stats(&self) -> QueueStats {
        let total_batches = self.len();
        let total_transactions = self.transaction_count();
        let failed_blocks_count = self.failed_blocks.len();

        let oldest_batch_height = self.pending_batches.front().map(|batch| batch.block_height);

        let newest_batch_height = self.pending_batches.back().map(|batch| batch.block_height);

        QueueStats {
            total_batches,
            total_transactions,
            failed_blocks_count,
            oldest_batch_height,
            newest_batch_height,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub total_batches: usize,
    pub total_transactions: usize,
    pub failed_blocks_count: usize,
    pub oldest_batch_height: Option<u64>,
    pub newest_batch_height: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_transaction(tx_index: u64) -> PendingTransaction {
        PendingTransaction {
            tx_hash: [u8::try_from(tx_index % 256).unwrap_or(0); 32],
            tx_bytes: vec![1, 2, 3],
            tx_index,
            events: vec![],
            retry_info: None,
        }
    }

    fn create_test_batch(block_height: u64, tx_count: usize) -> TransactionBatch {
        let transactions = (0..tx_count)
            .map(|i| create_test_transaction(i as u64))
            .collect();

        TransactionBatch::new(block_height, chrono::Utc::now(), transactions)
    }

    #[test]
    fn test_new_queue_is_empty() {
        let queue = TransactionQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.transaction_count(), 0);
    }

    #[test]
    fn test_enqueue_batch() {
        let mut queue = TransactionQueue::new();
        let batch = create_test_batch(1, 5);

        queue.enqueue_batch(batch);

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.transaction_count(), 5);

        let stats = queue.stats();
        assert_eq!(stats.total_batches, 1);
        assert_eq!(stats.total_transactions, 5);
        assert_eq!(stats.oldest_batch_height, Some(1));
        assert_eq!(stats.newest_batch_height, Some(1));
    }

    #[test]
    fn test_create_batch() {
        let mut queue = TransactionQueue::new();
        let transactions = vec![create_test_transaction(0), create_test_transaction(1)];

        queue.create_batch(1, chrono::Utc::now(), transactions);

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.transaction_count(), 2);
    }

    #[test]
    fn test_create_empty_batch() {
        let mut queue = TransactionQueue::new();

        queue.create_batch(1, chrono::Utc::now(), vec![]);

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_take_ready_batches() {
        let mut queue = TransactionQueue::new();
        queue.enqueue_batch(create_test_batch(1, 3));
        queue.enqueue_batch(create_test_batch(2, 2));

        let batches = queue.take_ready_batches();

        assert_eq!(batches.len(), 2);
        assert!(queue.is_empty());
        assert_eq!(batches[0].block_height, 1);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].block_height, 2);
        assert_eq!(batches[1].len(), 2);
    }

    #[test]
    fn test_mark_block_failed() {
        let mut queue = TransactionQueue::new();
        queue.enqueue_batch(create_test_batch(1, 3));

        queue.mark_block_failed(1);

        let ready_batches = queue.take_ready_batches();
        assert_eq!(ready_batches.len(), 0);
        assert_eq!(queue.len(), 1);

        assert!(queue.failed_blocks.contains_key(&1));
        assert_eq!(queue.failed_blocks.get(&1).unwrap().attempts, 1);
    }

    #[test]
    fn test_mark_block_failed_multiple_times() {
        let mut queue = TransactionQueue::new();

        queue.mark_block_failed(1);
        let first_retry = queue.failed_blocks.get(&1).unwrap().clone();

        queue.mark_block_failed(1);
        let second_retry = queue.failed_blocks.get(&1).unwrap().clone();

        assert!(second_retry.next_retry > first_retry.next_retry);
        assert_eq!(second_retry.attempts, 2);

        queue.mark_block_failed(1);
        let third_retry = queue.failed_blocks.get(&1).unwrap().clone();

        assert!(third_retry.next_retry > second_retry.next_retry);
        assert_eq!(third_retry.attempts, 3);
    }

    #[test]
    fn test_requeue_batch_with_retries() {
        let mut queue = TransactionQueue::new();
        let batch = create_test_batch(1, 3);
        let failed_tx_hash = batch.transactions[1].tx_hash;

        let failed_tx_hashes = vec![(failed_tx_hash, "Test error".to_string())];
        queue.requeue_batch_with_retries(batch, &failed_tx_hashes);

        assert_eq!(queue.len(), 1);

        let ready_batches = queue.take_ready_batches();
        assert_eq!(ready_batches.len(), 0);

        let all_batches = queue.take_all_batches();
        assert_eq!(all_batches.len(), 1);

        let tx_with_retry = &all_batches[0].transactions[1];
        assert!(tx_with_retry.retry_info.is_some());
        assert_eq!(tx_with_retry.retry_info.as_ref().unwrap().attempts, 1);

        let tx_without_retry_1 = &all_batches[0].transactions[0];
        let tx_without_retry_2 = &all_batches[0].transactions[2];
        assert!(tx_without_retry_1.retry_info.is_none());
        assert!(tx_without_retry_2.retry_info.is_none());
    }

    #[test]
    fn test_requeue_batch_with_empty_failures() {
        let mut queue = TransactionQueue::new();
        let batch = create_test_batch(1, 3);

        queue.requeue_batch_with_retries(batch.clone(), &[]);

        assert_eq!(queue.len(), 1);

        let ready_batches = queue.take_ready_batches();
        assert_eq!(ready_batches.len(), 1);
        assert_eq!(ready_batches[0].len(), 3);

        for tx in &ready_batches[0].transactions {
            assert!(tx.retry_info.is_none());
        }
    }

    #[test]
    fn test_take_all_batches() {
        let mut queue = TransactionQueue::new();
        queue.enqueue_batch(create_test_batch(1, 3));
        queue.enqueue_batch(create_test_batch(2, 2));

        let batches = queue.take_all_batches();

        assert_eq!(batches.len(), 2);
        assert!(queue.is_empty());
        assert_eq!(batches[0].block_height, 1);
        assert_eq!(batches[1].block_height, 2);
    }

    #[test]
    fn test_pending_transaction_is_ready() {
        let tx = create_test_transaction(1);
        assert!(tx.is_ready(Instant::now()));

        let mut tx_with_retry = create_test_transaction(2);
        tx_with_retry.retry_info = Some(RetryInfo {
            attempts: 1,
            next_retry: Instant::now() + Duration::from_millis(100),
        });

        assert!(!tx_with_retry.is_ready(Instant::now()));

        let future = Instant::now() + Duration::from_millis(200);
        assert!(tx_with_retry.is_ready(future));
    }

    #[test]
    fn test_mark_transaction_for_retry() {
        let mut tx = create_test_transaction(1);

        tx.mark_for_retry("First error");
        assert!(tx.retry_info.is_some());
        assert_eq!(tx.retry_info.as_ref().unwrap().attempts, 1);

        tx.mark_for_retry("Second error");
        assert_eq!(tx.retry_info.as_ref().unwrap().attempts, 2);

        tx.mark_for_retry("Third error");
        assert_eq!(tx.retry_info.as_ref().unwrap().attempts, 3);
    }

    #[test]
    fn test_transaction_batch_methods() {
        let empty_batch = TransactionBatch::new(1, chrono::Utc::now(), vec![]);
        assert!(empty_batch.is_empty());
        assert_eq!(empty_batch.len(), 0);

        let batch_with_txs = create_test_batch(2, 3);
        assert!(!batch_with_txs.is_empty());
        assert_eq!(batch_with_txs.len(), 3);
    }

    #[test]
    fn test_queue_stats() {
        let mut queue = TransactionQueue::new();

        let empty_stats = queue.stats();
        assert_eq!(empty_stats.total_batches, 0);
        assert_eq!(empty_stats.total_transactions, 0);
        assert_eq!(empty_stats.failed_blocks_count, 0);
        assert_eq!(empty_stats.oldest_batch_height, None);
        assert_eq!(empty_stats.newest_batch_height, None);

        queue.enqueue_batch(create_test_batch(1, 3));
        queue.enqueue_batch(create_test_batch(2, 2));

        queue.mark_block_failed(3);

        let stats = queue.stats();
        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_transactions, 5);
        assert_eq!(stats.failed_blocks_count, 1);
        assert_eq!(stats.oldest_batch_height, Some(1));
        assert_eq!(stats.newest_batch_height, Some(2));
    }
}
