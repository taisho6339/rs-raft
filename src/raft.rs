use std::borrow::{Borrow, BorrowMut};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

const RECONCILE_TICK_DURATION_MILLIS: u64 = 100;
const ELECTION_TIME_OUT_BASE_MILLIS: i64 = 150;

#[derive(Copy, Clone)]
pub enum RaftNodeRole {
    Dead,
    Leader,
    Follower,
    Candidate,
}

pub struct RaftConsensusState {
    current_role: RaftNodeRole,
    current_term: u64,
    election_timeout: Duration,
    last_heartbeat_time: DateTime<Utc>,
    voted_for: i64,
}

pub struct RaftReconciler {
    signal: Receiver<()>,
    state: Arc<Mutex<RaftConsensusState>>,
}

impl Default for RaftConsensusState {
    fn default() -> Self {
        let current_role = RaftNodeRole::Dead;
        let current_term = 0;
        let election_timeout = Duration::seconds(0);
        let last_heartbeat_time = Utc::now();
        let voted_for = -1;

        Self {
            voted_for,
            election_timeout,
            current_role,
            current_term,
            last_heartbeat_time,
        }
    }
}

impl RaftConsensusState {
    pub(crate) fn become_follower(&mut self, term: u64) {
        self.voted_for = -1;
        self.current_term = term;
        self.current_role = RaftNodeRole::Follower;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    fn become_candidate(&mut self) {
        // self.voted_for = sel
        self.current_term += 1;
        self.current_role = RaftNodeRole::Candidate;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    fn become_leader(&mut self) {}
}

impl RaftReconciler {
    pub fn new(state: Arc<Mutex<RaftConsensusState>>, signal: Receiver<()>) -> Self {
        Self {
            signal,
            state,
        }
    }

    fn current_role(&mut self) -> RaftNodeRole {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_role
    }

    fn reconcile_candidate(&mut self) {}

    fn reconcile_forwarder(&mut self) {
        let mut state = self.state.borrow_mut().lock().unwrap();
        let now = Utc::now();
        let duration = now.timestamp_millis() - state.last_heartbeat_time.timestamp_millis();
        if duration > state.election_timeout.num_milliseconds() {
            println!("[INFO] Become a candidate");
            state.become_candidate();
        }
    }

    pub async fn reconcile_loop(&mut self) {
        println!("[INFO] Start reconcile loop");
        let mut interval = interval(core::time::Duration::from_millis(RECONCILE_TICK_DURATION_MILLIS));
        loop {
            select! {
                _ = self.signal.changed() => {
                    println!("[INFO] Shutting down reconcile loop...");
                    return;
                }
                _ = interval.tick() => {
                    let role = self.current_role();
                    match role {
                        RaftNodeRole::Dead => {
                            println!("[INFO] Dead");
                        }
                        RaftNodeRole::Leader => {
                            println!("[INFO] Leader");
                        }
                        RaftNodeRole::Follower => {
                            println!("[INFO] Reconcile Follower");
                            self.reconcile_forwarder();
                        }
                        RaftNodeRole::Candidate => {
                            println!("[INFO] Reconcile Candidate");
                        }
                    }
                }
            }
        }
    }
}

fn randomized_timeout_duration(base_millis: i64) -> Duration {
    let mut rng = thread_rng();
    Duration::milliseconds(base_millis + rng.gen_range(0..=base_millis))
}