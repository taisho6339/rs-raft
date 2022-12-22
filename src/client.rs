use tonic::transport::{Channel, Endpoint};
use crate::rsraft::raft_client::RaftClient;

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
}