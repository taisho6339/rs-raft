use std::sync::{Arc, Mutex};
use chrono::Duration;
use tonic::transport::{Channel, Endpoint};
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
            let mut client = RaftClient::connect(Endpoint::from_static(p)).await.unwrap();
            clients.push(client);
        }
        Self {
            clients,
        }
    }

    pub fn request_vote(&self, req: RequestVoteRequest) {
        for mut c in self.clients.iter() {
            let r = req.clone();
            let mut c1 = c.clone();
            tokio::spawn(async move {
                let request = tonic::Request::new(r);
                let response = c1.request_vote(request).await;
                if response.is_err() {
                    return;
                }
                println!("RESPONSE={:?}", response.unwrap());
            });
        }
    }
}