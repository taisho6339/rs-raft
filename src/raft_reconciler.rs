use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

use crate::{ClusterInfo, RaftConsensusState};
use crate::raft_client::RaftServiceClient;
use crate::raft_state::RaftNodeRole;
use crate::raft_state::RaftNodeRole::Leader;
use crate::rsraft::{AppendEntriesRequest, RequestVoteRequest};

const RECONCILE_TICK_DURATION_MILLIS: u64 = 100;
const HEART_BEAT_TICK_DURATION_MILLIS: u64 = 50;

pub struct RaftReconciler {
    cluster_info: ClusterInfo,
    signal: Receiver<()>,
    state: Arc<Mutex<RaftConsensusState>>,
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

    fn current_term(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_term
    }

    fn reconcile_election_results(&mut self) {
        let mut state_ref = self.state.clone();
        let mut state = state_ref.borrow_mut().lock().unwrap();
        let granted_objective = (self.cluster_info.other_hosts.len() + 1) as i64;
        if 2 * state.received_granted >= granted_objective {
            println!("[INFO] Become the Leader");
            state.become_leader();
            self.spawn_heartbeat_loop();
        }
    }

    fn reconcile_election_timeout(&mut self) {
        let mut state_ref = self.state.clone();
        let mut state = state_ref.borrow_mut().lock().unwrap();
        let now = Utc::now();
        let duration = now.timestamp_millis() - state.last_heartbeat_time.timestamp_millis();
        if duration > state.election_timeout.num_milliseconds() {
            state.become_candidate();
            println!("[INFO] Become a candidate on term: {}", state.current_term);
            self.client.request_vote(RequestVoteRequest {
                candidate_id: String::from(self.cluster_info.node_id),
                term: state.current_term,
                last_log_index: 0,
                last_log_term: 0,
            }, state.election_timeout.num_milliseconds(), self.state.clone());
        }
    }

    pub fn spawn_heartbeat_loop(&mut self) {
        println!("[INFO] Start heart beat loop");
        let mut interval = interval(core::time::Duration::from_millis(HEART_BEAT_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        let state_ref = self.state.clone();
        let rsc = self.client.clone();
        let node_id = self.cluster_info.node_id;

        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = interval.tick() => {
                        println!("[INFO] Sending heartbeats...");
                        let mut _state_ref1 = state_ref.clone();
                        let mut _state_ref2 = state_ref.clone();
                        let state = _state_ref1.borrow_mut().lock().unwrap();
                        if state.current_role != Leader {
                            return;
                        }
                        let req = AppendEntriesRequest {
                            term: state.current_term,
                            leader_id: String::from(node_id),
                            leader_commit_index: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            logs: vec![]
                        };
                        rsc.heartbeats(req, HEART_BEAT_TICK_DURATION_MILLIS);
                    }
                }
            }
        });
    }

    pub async fn reconcile_loop(&mut self) {
        println!("[INFO] Start reconcile loop");
        let mut interval = interval(core::time::Duration::from_millis(RECONCILE_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        loop {
            select! {
                _ = ch.changed() => {
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
                            self.reconcile_election_timeout();
                        }
                        RaftNodeRole::Candidate => {
                            println!("[INFO] Reconcile Candidate");
                            self.reconcile_election_timeout();
                            self.reconcile_election_results();
                        }
                    }
                }
            }
        }
    }
}
