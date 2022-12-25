use std::borrow::BorrowMut;
use std::fmt::format;
use std::sync::{Arc, Mutex};
use tonic::Response;

use tonic::transport::{Channel, Endpoint};

use crate::raft_state::RaftConsensusState;
use crate::raft_state::RaftNodeRole::Candidate;
use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, RequestVoteRequest};
use crate::rsraft::raft_client::RaftClient;

#[derive(Clone)]
pub struct RaftServiceClient {
    clients: Vec<RaftClient<Channel>>,
}

impl RaftServiceClient {
    pub async fn new(hosts: Vec<&'static str>) -> Self {
        let mut clients = vec![];
        let peers = hosts.iter().map(|h| h as &str).collect::<Vec<&str>>();
        for p in peers.iter() {
            let client = RaftClient::connect(Endpoint::from_static(p)).await.unwrap();
            clients.push(client);
        }
        Self {
            clients,
        }
    }

    pub async fn append_entries(&self, peer_index: usize, req: AppendEntriesRequest, time_out_millis: u64) -> Result<Response<AppendEntriesResult>, ()> {
        // 最初は自分のlast log + 1
        // ※client-sideはsuccessがfalseでも受け取ったタイミングでreceived timeを更新する
        // crashからrecoveryしたときにcommitIndexを把握する方法はある？ <= 新リーダが教えてくれる？
        // heartbeatでcommitIndexをみて自分のcommitIndexを更新できる
        let mut c = self.clients[peer_index].clone();
        let mut r = tonic::Request::new(req);
        r.metadata_mut().insert("grpc-timeout", format!("{}m", time_out_millis).parse().unwrap());
        let response = c.append_entries(r).await;
        if response.is_err() {
            return Err(());
        }
        let message = response.unwrap();
        Ok(message)
    }

    pub fn heartbeats(&self, req: AppendEntriesRequest, time_out_millis: u64) {
        for c in self.clients.iter() {
            let r = req.clone();
            let mut c1 = c.clone();
            tokio::spawn(async move {
                let mut request = tonic::Request::new(r);
                request.metadata_mut().insert("grpc-timeout", format!("{}m", time_out_millis).parse().unwrap());
                let response = c1.append_entries(request).await;
                match response {
                    Ok(_) => {}
                    Err(e) => {
                        println!("[INFO] heartbeats failed, code: {}, message: {}", e.code(), e.message());
                    }
                }
            });
        }
    }

    pub fn request_vote(&self, req: RequestVoteRequest, time_out_millis: i64, state: Arc<Mutex<RaftConsensusState>>) {
        for c in self.clients.iter() {
            let r = req.clone();
            let mut c1 = c.clone();
            let mut s1 = state.clone();
            tokio::spawn(async move {
                let mut request = tonic::Request::new(r);
                request.metadata_mut().insert("grpc-timeout", format!("{}m", time_out_millis).parse().unwrap());
                let response = c1.request_vote(request).await;
                if response.is_err() {
                    return;
                }
                for res in response.iter() {
                    let message = res.get_ref();
                    let mut s = s1.borrow_mut().lock().unwrap();
                    if s.current_role != Candidate {
                        return;
                    }
                    if s.current_term < message.term {
                        s.become_follower(message.term);
                    }
                    if message.vote_granted {
                        s.received_granted += 1;
                    }
                };
            });
        }
    }
}