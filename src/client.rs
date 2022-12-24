use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use tonic::transport::{Channel, Endpoint};

use crate::raft::RaftNodeRole::Candidate;
use crate::RaftConsensusState;
use crate::rsraft::raft_client::RaftClient;
use crate::rsraft::RequestVoteRequest;

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

    pub fn request_vote(&self, req: RequestVoteRequest, granted_objective: i64, state: Arc<Mutex<RaftConsensusState>>) {
        for c in self.clients.iter() {
            let r = req.clone();
            let mut c1 = c.clone();
            let mut s1 = state.clone();
            tokio::spawn(async move {
                let request = tonic::Request::new(r);
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
                    if s.received_granted >= granted_objective {
                        println!("[INFO] Become the Leader");
                        s.become_leader();
                    }
                    return;
                };
            });
        }
    }
}