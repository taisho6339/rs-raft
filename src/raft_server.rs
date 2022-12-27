use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use tokio::sync::watch::Receiver;
use tonic::{Code, Request, Response, Status};
use tonic::transport::Server;

use crate::raft_state::RaftConsensusState;
use crate::raft_state::RaftNodeRole::Dead;
use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, CommandRequest, CommandResult, LeaderRequest, LeaderResult, LogsRequest, LogsResult, RequestVoteRequest, RequestVoteResult};
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
    async fn command(&self, request: Request<CommandRequest>) -> Result<Response<CommandResult>, Status> {
        let args = request.get_ref();
        let mut state_clone = self.raft_state.clone();
        let mut state = state_clone.borrow_mut().lock().unwrap();
        let success = state.apply_command_request(args);
        state.save_state_to_persistent_storage();
        Ok(Response::new(CommandResult {
            success
        }))
    }

    async fn leader(&self, _request: Request<LeaderRequest>) -> Result<Response<LeaderResult>, Status> {
        let mut state_clone = self.raft_state.clone();
        let state = state_clone.borrow_mut().lock().unwrap();
        Ok(Response::new(LeaderResult {
            leader: state.current_leader_id.clone(),
        }))
    }

    async fn logs(&self, _request: Request<LogsRequest>) -> Result<Response<LogsResult>, Status> {
        let mut state_clone = self.raft_state.clone();
        let state = state_clone.borrow_mut().lock().unwrap();
        return Ok(Response::new(LogsResult {
            logs: state.logs.clone(),
        }));
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResult>, Status> {
        let sc = self.raft_state.clone();
        let mut state = sc.lock().unwrap();
        if state.current_role == Dead {
            return Err(Status::new(Code::Unavailable, "This node is dead"));
        }
        let args = request.get_ref();
        let granted = state.apply_request_vote_request(args);
        state.save_state_to_persistent_storage();
        Ok(Response::new(RequestVoteResult {
            vote_granted: granted,
            term: state.current_term,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResult>, Status> {
        let sc = self.raft_state.clone();
        let mut state = sc.lock().unwrap();
        if state.current_role == Dead {
            return Err(Status::new(Code::Unavailable, "This node is dead"));
        }
        let args = request.get_ref();
        let success = state.apply_append_entries_request(args);
        state.save_state_to_persistent_storage();
        return Ok(Response::new(AppendEntriesResult {
            term: state.current_term,
            success,
        }));
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