use std::sync::{Arc, RwLock};

use futures::stream::{FuturesUnordered, StreamExt};
use tonic::transport::{Channel, Endpoint};

use crate::raft_state::RaftNodeRole::{Candidate, Leader};
use crate::RaftConsensusState;
use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, LogEntry, RequestVoteRequest, RequestVoteResult};
use crate::rsraft::raft_client::RaftClient;
use crate::storage::{ApplyStorage, PersistentStateStorage};

#[derive(Clone)]
pub struct RaftServiceClient {
    node_id: String,
    peers: Vec<&'static str>,
    clients: Vec<RaftClient<Channel>>,
}

impl RaftServiceClient {
    pub async fn new(node_id: String, peers: Vec<&'static str>) -> Self {
        let mut clients = vec![];
        let peers = peers.iter().map(|h| h as &str).collect::<Vec<&str>>();
        for p in peers.iter() {
            let client = RaftClient::connect(Endpoint::from_static(p)).await.unwrap();
            clients.push(client);
        }
        Self {
            node_id,
            clients,
            peers,
        }
    }

    fn gen_append_entries_requests<P: PersistentStateStorage, A: ApplyStorage>(&self, peer_index: usize, state: &Arc<RwLock<RaftConsensusState<P, A>>>) -> AppendEntriesRequest {
        let term;
        let leader_id;
        let prev_log_index;
        let prev_log_term;
        let leader_commit_index;
        let logs: Vec<LogEntry>;
        {
            let state = state.read().unwrap();
            let next_index = state.next_indexes[peer_index];
            prev_log_index = next_index - 1;
            if prev_log_index >= 0 {
                prev_log_term = state.logs[prev_log_index as usize].term;
            } else {
                prev_log_term = -1;
            }
            leader_id = self.node_id.clone();
            term = state.current_term;
            logs = (&state.logs)[next_index as usize..].to_vec();
            leader_commit_index = state.commit_index;
        }
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_term,
            prev_log_index,
            leader_commit_index,
            logs,
        }
    }

    async fn append_entries(&self, peer_index: usize, req: AppendEntriesRequest, timeout_millis: u64) -> Option<(usize, AppendEntriesResult)> {
        let mut c = self.clients[peer_index].clone();
        let mut r = tonic::Request::new(req);
        r.metadata_mut().insert("grpc-timeout", format!("{}m", timeout_millis).parse().unwrap());
        let response = c.append_entries(r).await;
        if response.is_err() {
            return None;
        }
        let message = response.unwrap();
        Some((peer_index, message.into_inner()))
    }

    pub async fn send_append_entries_over_cluster<P: PersistentStateStorage, A: ApplyStorage>(&self, state: &Arc<RwLock<RaftConsensusState<P, A>>>, timeout_millis: u64) {
        let reqs = self.peers.iter()
            .enumerate()
            .map(|(i, _)| self.gen_append_entries_requests(i, &state))
            .collect::<Vec<AppendEntriesRequest>>();

        let mut futures = FuturesUnordered::new();
        reqs.iter().enumerate().for_each(|(i, r)| {
            let req = r.clone();
            let fut = self.append_entries(i, req, timeout_millis);
            futures.push(fut);
        });

        while let Some(opt) = futures.next().await {
            let mut state = state.write().unwrap();
            if state.current_role != Leader {
                return;
            }
            if opt.is_none() {
                continue;
            }
            let (i, result) = opt.unwrap();
            state.apply_append_entries_result(i, result);
            state.save_state_to_persistent_storage();
        }
    }

    async fn request_vote(&self, peer_index: usize, req: RequestVoteRequest, timeout_millis: u64) -> Option<RequestVoteResult> {
        let mut c = self.clients[peer_index].clone();
        let r = req.clone();
        let mut request = tonic::Request::new(r);
        request.metadata_mut().insert("grpc-timeout", format!("{}m", timeout_millis).parse().unwrap());
        let response = c.request_vote(request).await;

        if response.is_err() {
            return None;
        }
        let result = response.unwrap().into_inner();
        Some(result)
    }

    pub async fn send_request_vote_over_cluster<P: PersistentStateStorage, A: ApplyStorage>(&self, state: &Arc<RwLock<RaftConsensusState<P, A>>>, timeout_millis: u64) {
        let current_term;
        let last_log_index;
        let last_log_term;
        {
            let state = state.read().unwrap();
            current_term = state.current_term;
            last_log_index = state.last_index();
            last_log_term = state.last_log_term();
        }
        let req = RequestVoteRequest {
            candidate_id: self.node_id.clone(),
            term: current_term,
            last_log_index,
            last_log_term,
        };

        let mut futures = FuturesUnordered::new();
        self.peers.iter().enumerate().for_each(|(i, _)| {
            let fut = self.request_vote(i, req.clone(), timeout_millis);
            futures.push(fut);
        });

        while let Some(result) = futures.next().await {
            let mut state = state.write().unwrap();
            if state.current_role != Candidate {
                return;
            }
            if result.is_none() {
                continue;
            }
            state.apply_request_vote_result(result.unwrap());
            state.save_state_to_persistent_storage();
        }
    }
}