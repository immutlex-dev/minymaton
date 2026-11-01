//! In-memory wake signals. Storage stays authoritative: a signal only tells a
//! waiting receive to look again.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

#[derive(Default)]
struct TickState {
    clock: u64,
    stream_ticks: HashMap<i64, u64>,
    sub_ticks: HashMap<i64, u64>,
    global_access_tick: u64,
}

impl TickState {
    fn next_tick(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    fn record_stream(&mut self, stream_id: i64) {
        let tick = self.next_tick();
        self.stream_ticks
            .entry(stream_id)
            .and_modify(|cur| *cur = (*cur).max(tick))
            .or_insert(tick);
    }

    fn record_sub(&mut self, subscription_id: i64) {
        let tick = self.next_tick();
        self.sub_ticks
            .entry(subscription_id)
            .and_modify(|cur| *cur = (*cur).max(tick))
            .or_insert(tick);
    }

    fn record_global_access(&mut self) {
        let tick = self.next_tick();
        self.global_access_tick = self.global_access_tick.max(tick);
    }
}

#[derive(Default)]
pub(crate) struct Signals {
    /// Monotonic per-stream, per-subscription, and global access ticks
    /// serialized under a single mutex so tick allocation, map update, and
    /// watch channel notification are atomic with respect to `tick()` and
    /// `binding_changed_since()` (`Sol RR-09`, `Sol RR-13`, `Sol RR-14`).
    ticks: Mutex<TickState>,
    /// Per stream: the highest committed sequence.
    commits: Mutex<HashMap<i64, watch::Sender<u64>>>,
    /// Per subscription: the newest ownership generation taken by a receive
    /// (or `i64::MAX` when the subscription is deleted).
    owners: Mutex<HashMap<i64, watch::Sender<i64>>>,
    /// Per subscription: the highest committed sequence acknowledged on that
    /// subscription (or `u64::MAX` when the subscription is deleted).
    acked: Mutex<HashMap<i64, watch::Sender<u64>>>,
    /// Per stream: monotonic counter bumped whenever stream membership changes
    /// or the stream is deleted.
    access: Mutex<HashMap<i64, watch::Sender<u64>>>,
    /// Per `agent_key`: the currently bound `subscription_id` (or `0` when
    /// unbound / deleted).
    agent_bindings: Mutex<HashMap<String, watch::Sender<i64>>>,
    /// Monotonic counter bumped whenever the active cluster enrollment changes.
    enrollment: watch::Sender<u64>,
    /// Latest known active cluster enrollment digest.
    cluster_enrollment: Mutex<Option<[u8; 32]>>,
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
    pub fn tick(&self) -> u64 {
        self.ticks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clock
    }

    /// Whether `stream_id`, `subscription_id`, or global access/enrollment
    /// changed after `since_tick` (`Sol RR-09`, `Sol RR-13`, `Sol RR-14`).
    pub fn binding_changed_since(
        &self,
        stream_id: i64,
        subscription_id: i64,
        since_tick: u64,
    ) -> bool {
        let ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        ticks.global_access_tick > since_tick
            || ticks.stream_ticks.get(&stream_id).copied().unwrap_or(0) > since_tick
            || ticks.sub_ticks.get(&subscription_id).copied().unwrap_or(0) > since_tick
    }

    pub fn commits(&self, stream_id: i64) -> watch::Receiver<u64> {
        subscribe(&self.commits, stream_id)
    }

    pub fn committed(&self, stream_id: i64, sequence: u64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        ticks.record_stream(stream_id);
        raise(&self.commits, stream_id, sequence);
    }

    pub fn owners(&self, subscription_id: i64) -> watch::Receiver<i64> {
        subscribe(&self.owners, subscription_id)
    }

    pub fn owner_taken(&self, subscription_id: i64, generation: i64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        if generation == i64::MAX {
            ticks.record_sub(subscription_id);
        }
        raise(&self.owners, subscription_id, generation);
    }

    pub fn acked(&self, subscription_id: i64) -> watch::Receiver<u64> {
        subscribe(&self.acked, subscription_id)
    }

    pub fn subscription_acked(&self, subscription_id: i64, sequence: u64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        ticks.record_sub(subscription_id);
        raise(&self.acked, subscription_id, sequence);
    }

    /// Wakes any active waiter on a deleted subscription and removes its sender.
    pub fn subscription_deleted(&self, subscription_id: i64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        ticks.record_sub(subscription_id);
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
        let removed_acked = self
            .acked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&subscription_id);
        if let Some(sender) = removed_acked {
            sender.send_if_modified(|latest| {
                if *latest != u64::MAX {
                    *latest = u64::MAX;
                    true
                } else {
                    false
                }
            });
        }
    }

    pub fn access_changed(&self, stream_id: i64) -> watch::Receiver<u64> {
        subscribe(&self.access, stream_id)
    }

    pub fn stream_access_changed(&self, stream_id: i64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        ticks.record_stream(stream_id);
        self.access
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(stream_id)
            .or_insert_with(|| watch::Sender::new(0))
            .send_modify(|v| *v = v.saturating_add(1));
    }

    pub fn agent_bound(&self, agent_key: &str) -> watch::Receiver<i64> {
        self.agent_bindings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(agent_key.to_owned())
            .or_insert_with(|| watch::Sender::new(0))
            .subscribe()
    }

    pub fn set_agent_bound(&self, agent_key: &str, subscription_id: i64) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = ticks.next_tick();
        self.agent_bindings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(agent_key.to_owned())
            .or_insert_with(|| watch::Sender::new(0))
            .send_modify(|latest| *latest = subscription_id);
    }

    pub fn enrollment_changed(&self) -> watch::Receiver<u64> {
        self.enrollment.subscribe()
    }

    pub fn set_enrollment(&self, digest: [u8; 32]) {
        let mut ticks = self.ticks.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = {
            let mut stored = self
                .cluster_enrollment
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if *stored != Some(digest) {
                *stored = Some(digest);
                true
            } else {
                false
            }
        };
        if changed {
            ticks.record_global_access();
            self.enrollment.send_modify(|v| *v = v.saturating_add(1));
        }
    }

    pub fn active_enrollment(&self) -> Option<[u8; 32]> {
        *self
            .cluster_enrollment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
