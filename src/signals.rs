use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::watch;

use crate::Filter;

#[derive(Debug)]
pub(crate) struct Signal {
    generation: watch::Sender<u64>,
}

impl Signal {
    fn new() -> Arc<Self> {
        let (generation, _) = watch::channel(0);
        Arc::new(Self { generation })
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub(crate) fn notify(&self) {
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// In-process notifications are only a latency hint; SQLite remains the source
/// of truth. Weak entries keep the registry bounded by live consumer handles.
#[derive(Debug)]
pub(crate) struct Signals {
    all_publishes: Arc<Signal>,
    topics: Mutex<HashMap<String, Weak<Signal>>>,
    consumers: Mutex<HashMap<i64, Weak<Signal>>>,
}

impl Signals {
    pub(crate) fn new() -> Self {
        Self {
            all_publishes: Signal::new(),
            topics: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn publishes_for(&self, filter: &Filter) -> Arc<Signal> {
        match filter {
            Filter::All => self.all_publishes.clone(),
            Filter::Topic(topic) => signal_for_key(&self.topics, topic.clone()),
        }
    }

    pub(crate) fn consumer(&self, id: i64) -> Arc<Signal> {
        signal_for_key(&self.consumers, id)
    }

    pub(crate) fn notify_publishes<'a>(&self, topics: impl IntoIterator<Item = Option<&'a str>>) {
        self.all_publishes.notify();

        let topics: HashSet<&str> = topics.into_iter().flatten().collect();
        let mut signals = self.topics.lock().unwrap();
        for topic in topics {
            if let Some(signal) = signals.get(topic).and_then(Weak::upgrade) {
                signal.notify();
            } else {
                signals.remove(topic);
            }
        }
    }

    pub(crate) fn notify_consumer(&self, id: i64) {
        let mut signals = self.consumers.lock().unwrap();
        if let Some(signal) = signals.get(&id).and_then(Weak::upgrade) {
            signal.notify();
        } else {
            signals.remove(&id);
        }
    }
}

fn signal_for_key<K>(signals: &Mutex<HashMap<K, Weak<Signal>>>, key: K) -> Arc<Signal>
where
    K: Eq + Hash,
{
    let mut signals = signals.lock().unwrap();
    if let Some(signal) = signals.get(&key).and_then(Weak::upgrade) {
        return signal;
    }
    let signal = Signal::new();
    signals.insert(key, Arc::downgrade(&signal));
    signal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_signals_are_shared_only_for_the_same_topic() {
        let signals = Signals::new();
        let jobs = signals.publishes_for(&Filter::topic("jobs"));
        let same_jobs = signals.publishes_for(&Filter::topic("jobs"));
        let metrics = signals.publishes_for(&Filter::topic("metrics"));

        assert!(Arc::ptr_eq(&jobs, &same_jobs));
        assert!(!Arc::ptr_eq(&jobs, &metrics));
    }

    #[test]
    fn expired_registry_entries_are_replaced() {
        let signals = Signals::new();
        let original = signals.consumer(7);
        let original_address = Arc::as_ptr(&original);
        drop(original);

        let replacement = signals.consumer(7);
        assert_ne!(Arc::as_ptr(&replacement), original_address);
    }

    #[tokio::test]
    async fn publishes_wake_all_and_matching_topic_subscribers() {
        let signals = Signals::new();
        let all = signals.publishes_for(&Filter::All);
        let jobs = signals.publishes_for(&Filter::topic("jobs"));
        let metrics = signals.publishes_for(&Filter::topic("metrics"));
        let mut all_rx = all.subscribe();
        let mut jobs_rx = jobs.subscribe();
        let metrics_rx = metrics.subscribe();

        signals.notify_publishes([Some("jobs"), Some("jobs"), None]);

        all_rx.changed().await.unwrap();
        jobs_rx.changed().await.unwrap();
        assert!(!metrics_rx.has_changed().unwrap());
    }
}
