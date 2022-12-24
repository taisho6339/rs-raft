use std::borrow::BorrowMut;
use std::future::Future;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use crate::client::RaftServiceClient;

use crate::raft::{ClusterInfo, RaftConsensusState, RaftReconciler};
use crate::server::{RaftServerConfig, RaftServerDaemon};

mod raft;
mod server;
mod client;
mod rsraft;

// pub mod rsraft {
//     tonic::include_proto!("rsraft"); // The string specified here must match the proto package name
// }

async fn run_process<F: Future<Output=()>>(close_signal: F, node_id: &'static str, other_hosts: Vec<&'static str>, server_config: RaftServerConfig) -> Result<()> {
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

    let client = RaftServiceClient::new(other_hosts.clone()).await;
    let cluster_info = ClusterInfo::new(node_id, other_hosts.clone());
    let mut reconciler = RaftReconciler::new(rx2, cluster_info, raft_state2, client);
    let r = tokio::spawn(async move {
        let _ = reconciler.reconcile_loop().await;
    });

    {
        let mut state = raft_state.borrow_mut().lock().unwrap();
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
    run_process(signal, this_node_id, other_hosts, config).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use tokio::sync::watch::channel;

    use super::*;

    #[tokio::test]
    async fn test() {
        let (tx, rx) = channel(());
        let mut rx1 = rx.clone();
        let mut rx2 = rx.clone();
        let mut rx3 = rx.clone();
        let close_signal_1 = async move {
            rx1.changed().await.unwrap();
        };
        let close_signal_2 = async move {
            rx2.changed().await.unwrap();
        };
        let close_signal_3 = async move {
            rx3.changed().await.unwrap();
        };
        let server_config_1 = RaftServerConfig {
            port: 8070,
        };
        let server_config_2 = RaftServerConfig {
            port: 8080,
        };
        let server_config_3 = RaftServerConfig {
            port: 8090,
        };
        let h1 = tokio::spawn(async move {
            run_process(
                close_signal_1,
                "localhost:8070",
                vec!["http://localhost:8080", "http://localhost:8090"],
                server_config_1,
            ).await.unwrap();
        });
        let h2 = tokio::spawn(async move {
            run_process(
                close_signal_2,
                "localhost:8080",
                vec!["http://localhost:8070", "http://localhost:8090"],
                server_config_2,
            ).await.unwrap();
        });
        let h3 = tokio::spawn(async move {
            run_process(
                close_signal_3,
                "localhost:8090",
                vec!["http://localhost:8070", "http://localhost:8080"],
                server_config_3,
            ).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(10000)).await;
        tx.send(()).unwrap();
        h1.await.unwrap();
        h2.await.unwrap();
        h3.await.unwrap();
    }
}