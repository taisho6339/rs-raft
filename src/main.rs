use std::borrow::BorrowMut;
use std::future::Future;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

use crate::raft_client::RaftServiceClient;
use crate::raft_reconciler::RaftReconciler;
use crate::raft_server::{RaftServerConfig, RaftServerDaemon};
use crate::raft_state::{ClusterInfo, RaftConsensusState};

mod raft_state;
mod raft_reconciler;
mod raft_server;
mod raft_client;
mod rsraft;

// pub mod rsraft {
//     tonic::include_proto!("rsraft"); // The string specified here must match the proto package name
// }

async fn run_process<F: Future<Output=()>>(close_signal: F, node_id: String, other_hosts: Vec<&'static str>, server_config: RaftServerConfig) -> Result<()> {
    let (tx, rx) = watch::channel(());
    let rx1 = rx.clone();
    let rx2 = rx.clone();
    let mut raft_state = Arc::new(Mutex::new(RaftConsensusState::default()));
    let raft_state1 = raft_state.clone();
    let raft_state2 = raft_state.clone();

    let mut server = RaftServerDaemon::new(server_config);
    let s = tokio::spawn(async move {
        let _ = server.start_server(rx1, raft_state1).await;
    });

    let client = RaftServiceClient::new(node_id.clone(), other_hosts.clone()).await;
    let cluster_info = ClusterInfo::new(node_id.clone(), other_hosts.clone());
    let mut reconciler = RaftReconciler::new(rx2, cluster_info, raft_state2, client);
    let r = tokio::spawn(async move {
        let _ = reconciler.reconcile_loop().await;
    });

    {
        let mut state = raft_state.borrow_mut().lock().unwrap();
        state.initialize_indexes(other_hosts.len());
        state.become_follower(0);
    }

    close_signal.await;
    tx.send(()).context("failed to send signal")?;
    s.await.unwrap();
    r.await.unwrap();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut sig_term = signal(SignalKind::terminate()).context("failed to setup SIGTERM channel")?;
    let mut sig_int = signal(SignalKind::interrupt()).context("failed to setup SIGINT channel")?;
    let config = RaftServerConfig {
        port: 8080,
    };
    let signal = async {
        select! {
            _ = sig_term.recv() => {
                println!("[INFO] Received SIGTERM");
            }
            _ = sig_int.recv() => {
                println!("[INFO] Received SIGINT");
            }
        }
    };
    let this_node_id = "localhost:8080";
    let other_hosts = vec!["http://localhost:8080"];
    run_process(signal, String::from(this_node_id), other_hosts, config).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::watch::channel;
    use tonic::transport::Endpoint;

    use crate::rsraft::{CommandRequest, LeaderRequest, LogsRequest};
    use crate::rsraft::raft_client::RaftClient;

    use super::*;

    async fn get_leader_host() -> String {
        let mut client = RaftClient::connect(Endpoint::from_static("http://localhost:8070")).await.unwrap();
        let leader;
        loop {
            let r = tonic::Request::new(LeaderRequest {});
            let res = client.leader(r).await.unwrap();
            let message = res.get_ref();
            if message.leader.len() > 0 {
                leader = format!("http://{}", message.leader.clone());
                break;
            }
        }
        leader
    }

    fn read_le_u64(input: &mut &[u8]) -> u64 {
        let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
        *input = rest;
        u64::from_le_bytes(int_bytes.try_into().unwrap())
    }

    async fn assert_single_leader() {
        let hosts = vec!["http://localhost:8070", "http://localhost:8080", "http://localhost:8090"];
        let mut results = vec![];
        for h in hosts.iter() {
            let mut client = RaftClient::connect(Endpoint::from_static(h)).await.unwrap();
            let r = tonic::Request::new(LeaderRequest {});
            let res = client.leader(r).await.unwrap();
            let message = res.get_ref();
            results.push(message.leader.clone());
        }
        let mut buf = vec![];
        for res in results.iter() {
            if !buf.contains(res) {
                buf.push(res.clone());
            }
        }
        assert_eq!(buf.len(), 1);
    }

    async fn assert_commit_log() {
        let hosts = vec!["http://localhost:8070", "http://localhost:8080", "http://localhost:8090"];
        // let mut results = vec![];
        for h in hosts.iter() {
            let mut client = RaftClient::connect(Endpoint::from_static(h)).await.unwrap();
            let r = tonic::Request::new(LogsRequest {});
            let res = client.logs(r).await.unwrap();
            let message = res.get_ref();
            let mut payload = message.logs[0].payload.as_slice();
            let value = read_le_u64(&mut payload);
            println!("{}", value);
        }
    }

    #[tokio::test]
    async fn test() {
        let (tx, rx) = channel(());
        let mut rx1 = rx.clone();
        let mut rx2 = rx.clone();
        let mut rx3 = rx.clone();
        let r1 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8070 };
            let _ = run_process(async move { let _ = rx1.changed().await; }, format!("localhost:{}", 8070), vec!["http://localhost:8080", "http://localhost:8090"], server_config)
                .await;
        });
        let r2 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8080 };
            let _ = run_process(async move { let _ = rx2.changed().await; }, format!("localhost:{}", 8080), vec!["http://localhost:8070", "http://localhost:8090"], server_config)
                .await;
        });
        let r3 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8090 };
            let _ = run_process(async move { let _ = rx3.changed().await; }, format!("localhost:{}", 8090), vec!["http://localhost:8070", "http://localhost:8080"], server_config)
                .await;
        });

        let leader = get_leader_host().await;
        assert_single_leader().await;

        let mut client = RaftClient::connect(leader).await.unwrap();
        let payload = (1 as u64).to_le_bytes().to_vec();
        let req = tonic::Request::new(CommandRequest {
            payload,
        });
        let res = client.command(req).await.unwrap();
        assert_eq!(res.get_ref().success, true);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_commit_log().await;

        tx.send(()).unwrap();
        r1.await.unwrap();
        r2.await.unwrap();
        r3.await.unwrap();
    }
}