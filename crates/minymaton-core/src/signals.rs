//! In-memory wake signals. Storage stays authoritative: a signal only tells a
//! waiting receive to look again.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

#[derive(Default)]
pub(crate) struct Signals {
    /// Per stream: the highest committed sequence.
    commits: Mutex<HashMap<i64, watch::Sender<u64>>>,
    /// Per subscription: the newest ownership generation taken by a receive
    /// (or `i64::MAX` when the subscription is deleted).
    owners: Mutex<HashMap<i64, watch::Sender<i64>>>,
    /// Subscriptions that currently have an active waiter parked in `Waiter::wait`.
    waiting: Mutex<HashMap<i64, usize>>,
    active: watch::Sender<usize>,
}

pub(crate) struct WaitGuard {
    signals: Arc<Signals>,
    subscription_id: i64,
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        let mut waiting = self
            .signals
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = waiting.get_mut(&self.subscription_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                waiting.remove(&self.subscription_id);
            }
        }
        self.signals.active.send_replace(waiting.values().sum());
    }
}

impl Signals {
    pub fn commits(&self, stream_id: i64) -> watch::Receiver<u64> {
        subscribe(&self.commits, stream_id)
    }

    pub fn committed(&self, stream_id: i64, sequence: u64) {
        raise(&self.commits, stream_id, sequence);
    }

    pub fn owners(&self, subscription_id: i64) -> watch::Receiver<i64> {
        subscribe(&self.owners, subscription_id)
    }

    pub fn owner_taken(&self, subscription_id: i64, generation: i64) {
        raise(&self.owners, subscription_id, generation);
    }

    /// Wakes any active waiter on a deleted subscription and removes its sender.
    pub fn subscription_deleted(&self, subscription_id: i64) {
        let removed = self
            .owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&subscription_id);
        if let Some(sender) = removed {
            sender.send_if_modified(|latest| {
                if *latest != i64::MAX {
                    *latest = i64::MAX;
                    true
                } else {
                    false
                }
            });
        }
    }

    pub fn enter_wait(self: &Arc<Self>, subscription_id: i64) -> WaitGuard {
        let mut waiting = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        *waiting.entry(subscription_id).or_insert(0) += 1;
        self.active.send_replace(waiting.values().sum());
        WaitGuard {
            signals: Arc::clone(self),
            subscription_id,
        }
    }

    pub fn active_waits(&self) -> watch::Receiver<usize> {
        self.active.subscribe()
    }

    pub fn waiting_set(&self) -> HashSet<i64> {
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

fn subscribe<T: Default>(
    senders: &Mutex<HashMap<i64, watch::Sender<T>>>,
    key: i64,
) -> watch::Receiver<T> {
    senders
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key)
        .or_insert_with(|| watch::Sender::new(T::default()))
        .subscribe()
}

/// Raises the signal to `value`; it never moves backwards, so a late signal
/// from an older writer cannot hide a newer one.
fn raise<T: Default + Ord + Copy>(
    senders: &Mutex<HashMap<i64, watch::Sender<T>>>,
    key: i64,
    value: T,
) {
    senders
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key)
        .or_insert_with(|| watch::Sender::new(T::default()))
        .send_if_modified(|latest| {
            if value > *latest {
                *latest = value;
                true
            } else {
                false
            }
        });
}
