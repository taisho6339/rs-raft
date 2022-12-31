use std::sync::{Arc, RwLock};

use chrono::Utc;
use log::info;
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::time::interval;

use crate::{ClusterInfo, RaftConsensusState};
use crate::raft_client::RaftServiceClient;
use crate::raft_state::RaftNodeRole;
use crate::raft_state::RaftNodeRole::Leader;
use crate::storage::{ApplyStorage, PersistentStateStorage};

const RECONCILE_TICK_DURATION_MILLIS: u64 = 50;
const APPEND_ENTRIES_TICK_DURATION_MILLIS: u64 = 100;

pub struct RaftReconciler<P: PersistentStateStorage, A: ApplyStorage> {
    state: Arc<RwLock<RaftConsensusState<P, A>>>,
    cluster_info: ClusterInfo,
    signal: Receiver<()>,
    client: RaftServiceClient,
}

impl<P: PersistentStateStorage, A: ApplyStorage> RaftReconciler<P, A> {
    pub fn new(signal: Receiver<()>, cluster_info: ClusterInfo, state: Arc<RwLock<RaftConsensusState<P, A>>>, client: RaftServiceClient) -> Self {
        Self {
            cluster_info,
            signal,
            state,
            client,
        }
    }

    fn current_role(&mut self) -> RaftNodeRole {
        let state = self.state.read().unwrap();
        state.current_role
    }

    fn received_granted(&mut self) -> i64 {
        let state = self.state.read().unwrap();
        state.received_granted
    }

    fn election_timeout_millis(&mut self) -> u64 {
        let state = self.state.read().unwrap();
        state.election_timeout.num_milliseconds() as u64
    }

    fn last_heartbeat_time_millis(&mut self) -> i64 {
        let state = self.state.read().unwrap();
        state.last_heartbeat_time.timestamp_millis()
    }

    fn update_commit_index(&mut self) {
        let mut state = self.state.write().unwrap();
        state.update_commit_index();
    }

    fn become_leader(&mut self) {
        let mut state = self.state.write().unwrap();
        state.become_leader(self.cluster_info.node_id.clone());
    }

    fn become_candidate(&mut self) {
        let node_id = self.cluster_info.node_id.clone();
        let mut state = self.state.write().unwrap();
        info!("Become a candidate on term: {}, {}", state.current_term, node_id);
        state.become_candidate(node_id);
    }

    pub fn spawn_request_votes(&mut self, timeout_millis: u64) {
        let node_id = self.cluster_info.node_id.clone();
        info!("spawn request votes: {}", node_id);
        let mut ch = self.signal.clone();
        let c = self.client.clone();
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ch.changed() => {
                        return;
                    }
                    _ = c.send_request_vote_over_cluster(&state_clone, timeout_millis) => {
                        return;
                    }
                }
            }
        });
    }

    pub fn spawn_append_entries_loop(&mut self, timeout_millis: u64) {
        let node_id = self.cluster_info.node_id.clone();
        info!("Start append entries loop: {}", node_id);
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
                        info!("Sending Append Entries requests...: {}", node_id);
                        {
                            let current_role;
                            current_role = state_clone.read().unwrap().current_role;
                            if current_role != Leader {
                                return;
                            }
                        }
                        rsc.send_append_entries_over_cluster(&state_clone, timeout_millis).await;
                    }
                }
            }
        });
    }

    fn reconcile_election_results(&mut self) {
        let granted_objective = (self.cluster_info.other_hosts.len() + 1) as i64;
        if 2 * self.received_granted() >= granted_objective {
            info!("Become the Leader: {}", self.cluster_info.node_id.clone());
            self.become_leader();
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

    fn reconcile_apply(&mut self) {
        let mut state = self.state.write().unwrap();
        state.apply_committed_logs();
    }

    pub async fn reconcile_loop(&mut self) {
        let node_id = self.cluster_info.node_id.clone();
        info!("Start reconcile loop: {}", node_id);
        let mut interval = interval(core::time::Duration::from_millis(RECONCILE_TICK_DURATION_MILLIS));
        let mut ch = self.signal.clone();
        loop {
            select! {
                _ = ch.changed() => {
                    info!("Shutting down reconcile loop...: {}", node_id);
                    return;
                }
                _ = interval.tick() => {
                    let role = self.current_role();
                    match role {
                        RaftNodeRole::Dead => {
                            info!("Dead: {}", node_id);
                        }
                        RaftNodeRole::Leader => {
                            info!("Reconcile Leader: {}", node_id);
                            self.reconcile_commit_index();
                            self.reconcile_apply();
                        }
                        RaftNodeRole::Follower => {
                            info!("Reconcile Follower: {}", node_id);
                            self.reconcile_election_timeout();
                            self.reconcile_apply();
                        }
                        RaftNodeRole::Candidate => {
                            info!("Reconcile Candidate: {}", node_id);
                            self.reconcile_election_timeout();
                            self.reconcile_election_results();
                        }
                    }
                }
            }
        }
    }
}
