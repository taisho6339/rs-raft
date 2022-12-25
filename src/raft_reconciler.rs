use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

use crate::{ClusterInfo, RaftConsensusState};
use crate::raft_client::RaftServiceClient;
use crate::raft_state::RaftNodeRole;
use crate::raft_state::RaftNodeRole::{Candidate, Leader};
use crate::rsraft::{AppendEntriesRequest, LogEntry, RequestVoteRequest};

const RECONCILE_TICK_DURATION_MILLIS: u64 = 100;
const HEART_BEAT_TICK_DURATION_MILLIS: u64 = 50;
const APPEND_ENTRIES_TICK_DURATION_MILLIS: u64 = 100;

pub struct RaftReconciler {
    state: Arc<Mutex<RaftConsensusState>>,
    cluster_info: ClusterInfo,
    signal: Receiver<()>,
    client: RaftServiceClient,
}

pub fn gen_append_entries_request(index: usize, node_id: String, state: Arc<Mutex<RaftConsensusState>>) -> Option<AppendEntriesRequest> {
    let mut state_ref = state.clone();
    let term;
    let leader_id;
    let prev_log_index;
    let prev_log_term;
    let leader_commit_index;
    let mut logs: Vec<LogEntry> = vec![];
    {
        let _state = state_ref.borrow_mut().lock().unwrap();
        let next_index = _state.next_indexes[index];
        let length = _state.logs.len();
        // No logs to send to the follower
        if next_index >= (length as i64) {
            return None;
        }

        if next_index >= 1 {
            prev_log_index = next_index - 1;
            prev_log_term = logs[prev_log_index as usize].term;
        } else {
            prev_log_index = -1;
            prev_log_term = -1;
        }
        term = _state.current_term;
        leader_id = String::from(node_id);
        logs = (&_state.logs)[next_index as usize..].to_vec();
        leader_commit_index = _state.commit_index;
    }

    Some(AppendEntriesRequest {
        term,
        leader_id,
        prev_log_term,
        prev_log_index,
        leader_commit_index,
        logs,
    })
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

    fn last_index_and_term(&mut self) -> (i64, i64) {
        let state = self.state.borrow_mut().lock().unwrap();

        (state.last_index(), state.last_log_term())
    }

    fn current_role(&mut self) -> RaftNodeRole {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_role
    }

    fn current_term(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.current_term
    }

    fn received_granted(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.received_granted
    }

    fn election_timeout_millis(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.election_timeout.num_milliseconds()
    }

    fn last_heartbeat_time_millis(&mut self) -> i64 {
        let state = self.state.borrow_mut().lock().unwrap();
        state.last_heartbeat_time.timestamp_millis()
    }

    fn update_commit_index(&mut self) {
        let mut state_ref = self.state.clone();
        let mut state = state_ref.borrow_mut().lock().unwrap();
        let mut indexes = state.match_indexes.clone();
        indexes.push(state.last_index());
        indexes.sort_by(|a, b| a.cmp(b));
        let mid_index = indexes.len() / 2;
        state.commit_index = indexes[mid_index];
    }

    fn become_leader(&mut self) {
        let mut state = self.state.borrow_mut().lock().unwrap();
        state.become_leader(String::from(self.cluster_info.node_id));
    }

    fn become_candidate(&mut self) {
        let mut state = self.state.borrow_mut().lock().unwrap();
        println!("[INFO] Become a candidate on term: {}", state.current_term);
        state.become_candidate();
    }

    pub fn spawn_request_votes(&mut self, timeout_millis: i64) {
        println!("[INFO] spawn request votes");
        let current_term = self.current_term();
        let (last_log_index, last_log_term) = self.last_index_and_term();
        let req = RequestVoteRequest {
            candidate_id: String::from(self.cluster_info.node_id),
            term: current_term,
            last_log_index,
            last_log_term,
        };
        let state_ref = self.state.clone();
        let c = self.client.clone();
        let hosts = self.cluster_info.other_hosts.clone();
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::new();
            hosts.iter().enumerate().for_each(|(i, _)| {
                let fut = c.request_vote(i, req.clone(), timeout_millis);
                futures.push(fut);
            });
            loop {
                match futures.next().await {
                    Some(result) => {
                        let mut _state_ref = state_ref.clone();
                        let mut state = _state_ref.borrow_mut().lock().unwrap();
                        if state.current_role != Candidate {
                            return;
                        }
                        let result = result.unwrap();
                        if result.term > state.current_term {
                            state.become_follower(result.term);
                            return;
                        }
                        if !result.vote_granted {
                            continue;
                        }
                        state.received_granted += 1;
                    }
                    None => {
                        return;
                    }
                }
            }
        });
    }

    pub fn spawn_append_entries_loop(&mut self, timeout_millis: u64) {
        println!("[INFO] spawn append entries loop");
        let mut interval = interval(core::time::Duration::from_millis(APPEND_ENTRIES_TICK_DURATION_MILLIS as u64));
        let mut ch = self.signal.clone();
        let rsc = self.client.clone();
        let other_hosts = self.cluster_info.other_hosts.clone();
        let node_id = self.cluster_info.node_id;
        let mut state_ref = self.state.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = interval.tick() => {
                        println!("[INFO] Append Entries");
                        let reqs;
                        {
                            let current_role;
                            current_role = state_ref.borrow_mut().lock().unwrap().current_role;
                            if current_role != Leader {
                                return;
                            }
                        }
                        reqs = other_hosts.iter()
                                .enumerate()
                                .map(|(i, _)| gen_append_entries_request(i, String::from(node_id), state_ref.clone()))
                                .collect::<Vec<Option<AppendEntriesRequest>>>();
                        let mut futures = FuturesUnordered::new();
                        reqs.iter().enumerate().for_each(|(i, r)| {
                            if r.is_none() {
                                return;
                            }
                            let req = r.clone().unwrap();
                            let fut = rsc.append_entries(i, req, timeout_millis);
                            futures.push(fut);
                        });
                        while let Some(opt) = futures.next().await {
                            if opt.is_none() {
                                continue;
                            }
                            let (i, result) = opt.unwrap();
                            let mut state = state_ref.borrow_mut().lock().unwrap();
                            if state.current_term < result.term {
                                state.become_follower(result.term);
                                continue;
                            }
                            if result.success {
                                state.next_indexes[i] = state.logs.len() as i64;
                                state.match_indexes[i] = (state.logs.len() - 1) as i64;
                            } else {
                                state.next_indexes[i] -= 1;
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn spawn_heartbeat_loop(&mut self) {
        println!("[INFO] Start heart beat loop");
        let mut interval = interval(core::time::Duration::from_millis(HEART_BEAT_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        let rsc = self.client.clone();
        let node_id = self.cluster_info.node_id;
        let mut state_ref = self.state.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = interval.tick() => {
                        println!("[INFO] Sending heartbeats...");
                        let current_term;
                        let leader_commit_index;
                        {
                            let state = state_ref.borrow_mut().lock().unwrap();
                            if state.current_role != Leader {
                                return;
                            }
                            current_term = state.current_term;
                            leader_commit_index = state.commit_index;
                        }
                        let req = AppendEntriesRequest {
                            term: current_term,
                            leader_id: String::from(node_id),
                            leader_commit_index: leader_commit_index,
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

    fn reconcile_election_results(&mut self) {
        let granted_objective = (self.cluster_info.other_hosts.len() + 1) as i64;
        if 2 * self.received_granted() >= granted_objective {
            println!("[INFO] Become the Leader");
            self.become_leader();
            self.spawn_heartbeat_loop();
            self.spawn_append_entries_loop(APPEND_ENTRIES_TICK_DURATION_MILLIS);
        }
    }

    fn reconcile_election_timeout(&mut self) {
        let now = Utc::now();
        let election_timeout_millis = self.election_timeout_millis();
        let last_heartbeat_time = self.last_heartbeat_time_millis();
        let duration = now.timestamp_millis() - last_heartbeat_time;
        if duration > election_timeout_millis {
            self.become_candidate();
            self.spawn_request_votes(election_timeout_millis);
        }
    }

    fn reconcile_commit_index(&mut self) {
        self.update_commit_index();
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
                            self.reconcile_commit_index();
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
