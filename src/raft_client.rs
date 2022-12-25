use tonic::transport::{Channel, Endpoint};

use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, RequestVoteRequest, RequestVoteResult};
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

    pub async fn request_vote(&self, peer_index: usize, req: RequestVoteRequest, time_out_millis: i64) -> Option<RequestVoteResult> {
        let mut c = self.clients[peer_index].clone();
        let r = req.clone();
        let mut request = tonic::Request::new(r);
        request.metadata_mut().insert("grpc-timeout", format!("{}m", time_out_millis).parse().unwrap());
        let response = c.request_vote(request).await;

        if response.is_err() {
            return None;
        }
        let result = response.unwrap().into_inner();
        Some(result)
    }

    pub async fn append_entries(&self, peer_index: usize, req: AppendEntriesRequest, time_out_millis: u64) -> Option<(usize, AppendEntriesResult)> {
        let mut c = self.clients[peer_index].clone();
        let mut r = tonic::Request::new(req);
        r.metadata_mut().insert("grpc-timeout", format!("{}m", time_out_millis).parse().unwrap());
        let response = c.append_entries(r).await;
        if response.is_err() {
            return None;
        }
        let message = response.unwrap();
        Some((peer_index, message.into_inner()))
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
}