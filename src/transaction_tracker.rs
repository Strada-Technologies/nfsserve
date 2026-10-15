use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// Housekeeping walks every tracked transaction under the lock that all RPCs
/// share, so it runs at most this often rather than on every call.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);

/// `TransactionTracker` tracks the state of transactions to detect retransmissions.
pub struct TransactionTracker {
    retention_period: Duration,
    transactions: Mutex<Transactions>,
}

struct Transactions {
    states: HashMap<(u32, String), TransactionState>,
    last_housekeeping: Instant,
}

impl TransactionTracker {
    pub fn new(retention_period: Duration) -> Self {
        Self {
            retention_period,
            transactions: Mutex::new(Transactions {
                states: HashMap::new(),
                last_housekeeping: Instant::now(),
            }),
        }
    }

    /// Checks if the transaction is a retransmission.
    /// If it's a new transaction, it is marked as `InProgress`.
    ///
    /// Returns `true` if the transaction is a retransmission, `false` otherwise.
    pub fn is_retransmission(&self, xid: u32, client_addr: &str) -> bool {
        let key = (xid, client_addr.to_string());
        let mut transactions = self
            .transactions
            .lock()
            .expect("unable to unlock transactions mutex");

        if transactions.last_housekeeping.elapsed() >= HOUSEKEEPING_INTERVAL {
            housekeeping(&mut transactions.states, self.retention_period);
            transactions.last_housekeeping = Instant::now();
        }

        if let std::collections::hash_map::Entry::Vacant(e) = transactions.states.entry(key) {
            e.insert(TransactionState::InProgress);
            false
        } else {
            true
        }
    }

    /// Marks the transaction as processed.
    pub fn mark_processed(&self, xid: u32, client_addr: &str) {
        let key = (xid, client_addr.to_string());
        let completion_time = SystemTime::now();
        let mut transactions = self
            .transactions
            .lock()
            .expect("unable to unlock transactions mutex");
        if let Some(tx) = transactions.states.get_mut(&key) {
            *tx = TransactionState::Completed(completion_time);
        }
    }
}

fn housekeeping(transactions: &mut HashMap<(u32, String), TransactionState>, max_age: Duration) {
    let mut cutoff = SystemTime::now() - max_age;
    transactions.retain(|_, v| match v {
        TransactionState::InProgress => true,
        TransactionState::Completed(completion_time) => completion_time >= &mut cutoff,
    });
}

pub enum TransactionState {
    InProgress,
    Completed(SystemTime),
}
