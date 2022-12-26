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
    use std::fmt::format;
    use std::pin::Pin;
    use std::thread::sleep;
    use std::time::Duration;
    use futures::future::join_all;

    use tokio::sync::watch::{channel, Receiver};

    use super::*;

    fn gen_process(rx: Receiver<()>, port: u16, other_hosts: Vec<&'static str>) -> Pin<Box<dyn Future<Output=Result<()>>>> {
        let mut rx1 = rx.clone();
        let close_signal = async move {
            rx1.changed().await.unwrap();
        };
        let server_config = RaftServerConfig {
            port,
        };
        Box::pin(run_process(
            close_signal,
            format!("localhost:{}", port),
            other_hosts,
            server_config,
        ))
    }

    #[tokio::test]
    async fn test() {
        let (tx, rx) = channel(());
        let p1 = gen_process(rx.clone(), 8070, vec!["http://localhost:8080", "http://localhost:8090"]);
        let p2 = gen_process(rx.clone(), 8080, vec!["http://localhost:8070", "http://localhost:8090"]);
        let p3 = gen_process(rx.clone(), 8090, vec!["http://localhost:8070", "http://localhost:8080"]);

        let r1 = tokio::spawn(async move {
            p1.await.unwrap();
        });
        let r2 = tokio::spawn(async move {
            p2.await.unwrap();
        });
        let r3 = tokio::spawn(async move {
            p3.await.unwrap();
        });

        // let mut rx1 = rx.clone();
        // let mut rx2 = rx.clone();
        // let mut rx3 = rx.clone();
        // let close_signal_1 = async move {
        //     rx1.changed().await.unwrap();
        // };
        // let close_signal_2 = async move {
        //     rx2.changed().await.unwrap();
        // };
        // let close_signal_3 = async move {
        //     rx3.changed().await.unwrap();
        // };
        // let server_config_1 = RaftServerConfig {
        //     port: 8070,
        // };
        // let server_config_2 = RaftServerConfig {
        //     port: 8080,
        // };
        // let server_config_3 = RaftServerConfig {
        //     port: 8090,
        // };
        // let h1 = tokio::spawn(async move {
        //     run_process(
        //         close_signal_1,
        //         String::from("localhost:8070"),
        //         vec!["http://localhost:8080", "http://localhost:8090"],
        //         server_config_1,
        //     ).await.unwrap();
        // });
        // let h2 = tokio::spawn(async move {
        //     run_process(
        //         close_signal_2,
        //         "localhost:8080",
        //         vec!["http://localhost:8070", "http://localhost:8090"],
        //         server_config_2,
        //     ).await.unwrap();
        // });
        // let h3 = tokio::spawn(async move {
        //     run_process(
        //         close_signal_3,
        //         "localhost:8090",
        //         vec!["http://localhost:8070", "http://localhost:8080"],
        //         server_config_3,
        //     ).await.unwrap();
        // });

        tokio::time::sleep(Duration::from_millis(10000)).await;
        tx.send(()).unwrap();
        r1.await.unwrap();
        r2.await.unwrap();
        r3.await.unwrap();
    }
}