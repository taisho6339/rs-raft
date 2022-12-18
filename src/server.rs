use std::sync::{Arc, Mutex};
use anyhow::Context;

use tokio::sync::watch::Receiver;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use crate::RaftConsensusState;
use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, RequestVoteRequest, RequestVoteResult};
use crate::rsraft::raft_server::Raft;
use crate::rsraft::raft_server::RaftServer;

#[derive(Debug)]
pub struct RaftServerConfig {
    pub(crate) port: u16,
}

impl Default for RaftServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
        }
    }
}

#[derive(Debug)]
pub struct RaftServerDaemon {
    config: RaftServerConfig,
}

pub struct RaftServerHandler {
    raft_state: Arc<Mutex<RaftConsensusState>>,
}

#[tonic::async_trait]
impl Raft for RaftServerHandler {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResult>, Status> {
        let reply = RequestVoteResult {
            term: 1,
            vote_granted: true,
        };

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResult>, Status> {
        let reply = AppendEntriesResult {
            term: 1,
            success: true,
        };

        Ok(Response::new(reply))
    }
}

impl RaftServerDaemon {
    pub fn new(config: RaftServerConfig) -> Self {
        Self {
            config,
        }
    }

    pub async fn start_server(&mut self, mut signal: Receiver<()>, raft_state: Arc<Mutex<RaftConsensusState>>) -> anyhow::Result<()> {
        let conf = &self.config;
        let addr = format!("[::1]:{}", conf.port).parse().context("failed to parse addr")?;
        let handler = RaftServerHandler {
            raft_state
        };
        println!("[INFO] Starting gRPC server...");
        Server::builder()
            .add_service(RaftServer::new(handler))
            .serve_with_shutdown(addr, async {
                let _ = signal.changed().await;
                println!("[INFO] Shutting down gRPC server...");
            })
            .await
            .context("failed to serve")?;

        Ok(())
    }
}