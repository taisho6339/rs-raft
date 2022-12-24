use std::sync::{Arc, Mutex};

use anyhow::Context;
use chrono::Utc;
use tokio::sync::watch::Receiver;
use tonic::{Code, Request, Response, Status};
use tonic::transport::Server;

use crate::raft_state::RaftConsensusState;
use crate::raft_state::RaftNodeRole::Dead;
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
        let sc = self.raft_state.clone();
        let mut s = sc.lock().unwrap();
        if s.current_role == Dead {
            return Err(Status::new(Code::Unavailable, "This node is dead"));
        }

        let args = request.get_ref();
        if args.term > s.current_term {
            s.become_follower(args.term);
        }

        // FIXME:
        let (last_log_term, last_log_index) = (0, 0);
        let acceptable = args.term == s.current_term &&
            (args.candidate_id == s.voted_for || s.voted_for == "") &&
            (args.last_log_term > last_log_term || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index));

        if acceptable {
            s.voted_for = args.candidate_id.clone();
            Ok(Response::new(RequestVoteResult {
                vote_granted: true,
                term: s.current_term,
            }))
        } else {
            Ok(Response::new(RequestVoteResult {
                vote_granted: false,
                term: s.current_term,
            }))
        }
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResult>, Status> {
        let sc = self.raft_state.clone();
        let mut state = sc.lock().unwrap();
        let args = request.get_ref();

        // heartbeat
        if args.logs.len() == 0 {
            state.last_heartbeat_time = Utc::now();
            return Ok(Response::new(AppendEntriesResult {
                term: state.current_term,
                success: true,
            }));
        };

        // append entries

        Ok(Response::new(AppendEntriesResult {
            term: state.current_term,
            success: true,
        }))
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