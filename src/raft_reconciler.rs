use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

use crate::{ClusterInfo, RaftConsensusState};
use crate::raft_client::RaftServiceClient;
use crate::raft_state::RaftNodeRole;
use crate::raft_state::RaftNodeRole::Leader;

const RECONCILE_TICK_DURATION_MILLIS: u64 = 100;
const HEART_BEAT_TICK_DURATION_MILLIS: u64 = 50;
const APPEND_ENTRIES_TICK_DURATION_MILLIS: u64 = 100;

pub struct RaftReconciler {
    state: Arc<Mutex<RaftConsensusState>>,
    cluster_info: ClusterInfo,
    signal: Receiver<()>,
    client: RaftServiceClient,
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

    fn received_granted(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.received_granted
    }

    fn election_timeout_millis(&mut self) -> u64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.election_timeout.num_milliseconds() as u64
    }

    fn last_heartbeat_time_millis(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.last_heartbeat_time.timestamp_millis()
    }

    fn update_commit_index(&mut self) {
        let mut state_ref = self.state.clone();
        let mut state = state_ref.borrow_mut().lock().unwrap();
        state.update_commit_index();
    }

    fn become_leader(&mut self) {
        let mut state = self.state.borrow_mut().lock().unwrap();
        state.become_leader(self.cluster_info.node_id.clone());
    }

    fn become_candidate(&mut self) {
        let node_id = self.cluster_info.node_id.clone();
        let mut state = self.state.borrow_mut().lock().unwrap();
        println!("[INFO] Become a candidate on term: {}, {}", state.current_term, node_id);
        state.become_candidate(node_id);
    }

    pub fn spawn_request_votes(&mut self, timeout_millis: u64) {
        let node_id = self.cluster_info.node_id.clone();
        println!("[INFO] spawn request votes: {}", node_id);
        let mut ch = self.signal.clone();
        let c = self.client.clone();
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = c.send_request_vote_over_cluster(state_clone.clone(), timeout_millis) => {
                        return;
                    }
                }
            }
        });
    }

    pub fn spawn_append_entries_loop(&mut self, timeout_millis: u64) {
        let node_id = self.cluster_info.node_id.clone();
        println!("[INFO] Start append entries loop: {}", node_id);
        let mut interval = interval(core::time::Duration::from_millis(APPEND_ENTRIES_TICK_DURATION_MILLIS as u64));
        let mut ch = self.signal.clone();
        let rsc = self.client.clone();
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = interval.tick() => {
                        println!("[INFO] Sending Append Entries requests...: {}", node_id);
                        {
                            let current_role;
                            let mut state_clone = state_clone.clone();
                            current_role = state_clone.borrow_mut().lock().unwrap().current_role;
                            if current_role != Leader {
                                return;
                            }
                        }
                        let state_clone = state_clone.clone();
                        rsc.send_append_entries_over_cluster(state_clone, timeout_millis).await;
                    }
                }
            }
        });
    }

    pub fn spawn_heartbeat_loop(&mut self) {
        let node_id = self.cluster_info.node_id.clone();
        println!("[INFO] Start heart beat loop: {}", node_id);
        let mut interval = interval(core::time::Duration::from_millis(HEART_BEAT_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        let rsc = self.client.clone();
        let state_clone = self.state.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = interval.tick() => {
                        println!("[INFO] Sending heartbeats...: {}", node_id);
                        {
                            let current_role;
                            let mut state_clone = state_clone.clone();
                            current_role = state_clone.borrow_mut().lock().unwrap().current_role;
                            if current_role != Leader {
                                return;
                            }
                        }
                        rsc.send_heartbeat_over_cluster(state_clone.clone(), HEART_BEAT_TICK_DURATION_MILLIS).await;
                    }
                }
            }
        });
    }

    fn reconcile_election_results(&mut self) {
        let granted_objective = (self.cluster_info.other_hosts.len() + 1) as i64;
        if 2 * self.received_granted() >= granted_objective {
            println!("[INFO] Become the Leader: {}", self.cluster_info.node_id.clone());
            self.become_leader();
            self.spawn_heartbeat_loop();
            self.spawn_append_entries_loop(APPEND_ENTRIES_TICK_DURATION_MILLIS);
        }
    }

    fn reconcile_election_timeout(&mut self) {
        let now = Utc::now();
        let election_timeout_millis = self.election_timeout_millis();
        let last_heartbeat_time = self.last_heartbeat_time_millis();
        let duration = (now.timestamp_millis() - last_heartbeat_time) as u64;
        if duration > election_timeout_millis {
            self.become_candidate();
            self.spawn_request_votes(election_timeout_millis);
        }
    }

    fn reconcile_commit_index(&mut self) {
        self.update_commit_index();
    }

    pub async fn reconcile_loop(&mut self) {
        let node_id = self.cluster_info.node_id.clone();
        println!("[INFO] Start reconcile loop: {}", node_id);
        let mut interval = interval(core::time::Duration::from_millis(RECONCILE_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        loop {
            select! {
                _ = ch.changed() => {
                    println!("[INFO] Shutting down reconcile loop...: {}", node_id);
                    return;
                }
                _ = interval.tick() => {
                    let role = self.current_role();
                    match role {
                        RaftNodeRole::Dead => {
                            println!("[INFO] Dead");
                        }
                        RaftNodeRole::Leader => {
                            println!("[INFO] Reconcile Leader: {}", node_id);
                            self.reconcile_commit_index();
                        }
                        RaftNodeRole::Follower => {
                            println!("[INFO] Reconcile Follower: {}", node_id);
                            self.reconcile_election_timeout();
                        }
                        RaftNodeRole::Candidate => {
                            println!("[INFO] Reconcile Candidate: {}", node_id);
                            self.reconcile_election_timeout();
                            self.reconcile_election_results();
                        }
                    }
                }
            }
        }
    }
}
