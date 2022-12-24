use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

use crate::client::RaftServiceClient;
use crate::rsraft::RequestVoteRequest;

const RECONCILE_TICK_DURATION_MILLIS: u64 = 100;
const ELECTION_TIME_OUT_BASE_MILLIS: i64 = 150;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum RaftNodeRole {
    Dead,
    Leader,
    Follower,
    Candidate,
}

pub struct RaftConsensusState {
    pub current_role: RaftNodeRole,
    pub current_term: i64,
    pub election_timeout: Duration,
    pub last_heartbeat_time: DateTime<Utc>,
    pub voted_for: String,
    pub received_granted: i64,
}

pub struct ClusterInfo {
    pub node_id: &'static str,
    pub other_hosts: Vec<&'static str>,
}

impl ClusterInfo {
    pub fn new(node_id: &'static str, hosts: Vec<&'static str>) -> Self {
        Self {
            node_id,
            other_hosts: hosts,
        }
    }
}

pub struct RaftReconciler {
    cluster_info: ClusterInfo,
    signal: Receiver<()>,
    state: Arc<Mutex<RaftConsensusState>>,
    client: RaftServiceClient,
}

impl Default for RaftConsensusState {
    fn default() -> Self {
        let current_role = RaftNodeRole::Dead;
        let current_term = 0;
        let election_timeout = Duration::seconds(0);
        let last_heartbeat_time = Utc::now();
        let voted_for = String::from("");
        let received_granted = 0;

        Self {
            voted_for,
            election_timeout,
            current_role,
            current_term,
            last_heartbeat_time,
            received_granted,
        }
    }
}

impl RaftConsensusState {
    pub(crate) fn become_follower(&mut self, term: i64) {
        self.voted_for = String::from("");
        self.received_granted = 0;
        self.current_term = term;
        self.current_role = RaftNodeRole::Follower;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    fn become_candidate(&mut self) {
        // self.voted_for = sel
        self.current_term += 1;
        self.current_role = RaftNodeRole::Candidate;
        self.received_granted = 1;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    pub(crate) fn become_leader(&mut self) {
        self.voted_for = String::from("");
        self.received_granted = 0;
        self.current_role = RaftNodeRole::Leader;
    }
}

impl RaftReconciler {
    pub fn new(signal: Receiver<()>, cluster_info: ClusterInfo, state: Arc<Mutex<RaftConsensusState>>, client: RaftServiceClient) -> Self {
        Self {
            cluster_info,
            signal,
            state,
            client,
        }
    }

    fn current_role(&mut self) -> RaftNodeRole {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_role
    }

    fn current_term(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_term
    }

    fn reconcile_candidate(&mut self) {}

    fn reconcile_forwarder(&mut self) {
        let s1 = self.state.clone();
        let mut state = self.state.borrow_mut().lock().unwrap();
        let now = Utc::now();
        let duration = now.timestamp_millis() - state.last_heartbeat_time.timestamp_millis();
        if duration > state.election_timeout.num_milliseconds() {
            println!("[INFO] Become a candidate");
            state.become_candidate();
            self.client.request_vote(RequestVoteRequest {
                candidate_id: String::from(self.cluster_info.node_id),
                term: state.current_term,
                last_log_index: 0,
                last_log_term: 0,
            }, state.election_timeout.num_milliseconds(), &self.cluster_info, s1);
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
                            println!("[INFO] Reconcile Leader");
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