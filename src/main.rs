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
mod util;

// pub mod rsraft {
//     tonic::include_proto!("rsraft"); // The string specified here must match the proto package name
// }

async fn run_process<F: Future<Output=()>>(close_signal: F, node_id: String, other_hosts: Vec<&'static str>, server_config: RaftServerConfig, raft_state: Arc<Mutex<RaftConsensusState>>) -> Result<()> {
    let (tx, rx) = watch::channel(());
    let rx1 = rx.clone();
    let rx2 = rx.clone();
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
        let mut raft_state_clone = raft_state.clone();
        let mut state = raft_state_clone.borrow_mut().lock().unwrap();
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
    let state = RaftConsensusState::default();
    let raft_state = Arc::new(Mutex::new(state));
    run_process(signal, String::from(this_node_id), other_hosts, config, raft_state).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;
    use ntest::timeout;

    use tokio::sync::watch::channel;
    use tokio::time::interval;
    use tonic::Response;
    use crate::raft_state::RaftNodeRole::{Dead, Follower, Leader};

    use crate::rsraft::{CommandRequest, CommandResult, LogEntry};
    use crate::rsraft::raft_client::RaftClient;

    use super::*;

    fn leader_host(s1: Arc<Mutex<RaftConsensusState>>) -> String {
        let mut s1_clone = s1.clone();
        let state = s1_clone.borrow_mut().lock().unwrap();

        state.current_leader_id.clone()
    }

    async fn submit_command(s1: Arc<Mutex<RaftConsensusState>>, payload: u64) -> Response<CommandResult> {
        let leader_host = format!("http://{}", leader_host(s1.clone()));
        let mut client = RaftClient::connect(leader_host).await.unwrap();
        let payload = (payload as u64).to_le_bytes().to_vec();
        let req = tonic::Request::new(CommandRequest {
            payload,
        });
        return client.command(req).await.unwrap();
    }

    async fn eventually_assert<F>(mut f: F, timout_millis: u64) where
        F: FnMut() -> bool {
        let mut loop_interval = interval(Duration::from_millis(10));
        loop {
            select! {
                _ = tokio::time::sleep(Duration::from_millis(timout_millis)) => {
                    panic!("timeout in this test case");
                }
                _ = loop_interval.tick() => {
                    let success = f();
                    if success {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    #[tokio::test]
    #[timeout(5000)]
    async fn test() {
        // Initialize
        let (tx, rx) = channel(());
        let mut rx1_clone = rx.clone();
        let mut rx2_clone = rx.clone();
        let mut rx3_clone = rx.clone();
        let s1 = Arc::new(Mutex::new(RaftConsensusState::default()));
        let s2 = Arc::new(Mutex::new(RaftConsensusState::default()));
        let s3 = Arc::new(Mutex::new(RaftConsensusState::default()));
        let mut states_map = HashMap::new();
        states_map.insert("localhost:8070", s1.clone());
        states_map.insert("localhost:8080", s2.clone());
        states_map.insert("localhost:8090", s3.clone());
        let (s1_clone, s2_clone, s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        let r1 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8070 };
            let _ = run_process(async move { let _ = rx1_clone.changed().await; }, String::from("localhost:8070"), vec!["http://localhost:8080", "http://localhost:8090"], server_config, s1_clone)
                .await;
        });
        let r2 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8080 };
            let _ = run_process(async move { let _ = rx2_clone.changed().await; }, String::from("localhost:8080"), vec!["http://localhost:8070", "http://localhost:8090"], server_config, s2_clone)
                .await;
        });
        let r3 = tokio::spawn(async move {
            let server_config = RaftServerConfig { port: 8090 };
            let _ = run_process(async move { let _ = rx3_clone.changed().await; }, String::from("localhost:8090"), vec!["http://localhost:8070", "http://localhost:8080"], server_config, s3_clone)
                .await;
        });

        // Make sure the single Leader is elected
        let (mut s1_clone, mut s2_clone, mut s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        eventually_assert(move || {
            let s1 = s1_clone.borrow_mut().lock().unwrap();
            let s2 = s2_clone.borrow_mut().lock().unwrap();
            let s3 = s3_clone.borrow_mut().lock().unwrap();
            if s1.current_leader_id == "" {
                return false;
            }
            let unified_leader = (s1.current_leader_id == s2.current_leader_id) && (s1.current_leader_id == s3.current_leader_id);
            let unified_term = (s1.current_term == s2.current_term) && (s1.current_term == s3.current_term);
            return unified_leader && unified_term;
        }, 1000).await;

        // Send command
        let res = submit_command(s1.clone(), 1).await;
        assert_eq!(res.get_ref().success, true);
        let res = submit_command(s1.clone(), 2).await;
        assert_eq!(res.get_ref().success, true);

        // Commit check
        let (mut s1_clone, mut s2_clone, mut s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        eventually_assert(move || {
            let s1 = s1_clone.borrow_mut().lock().unwrap();
            let s2 = s2_clone.borrow_mut().lock().unwrap();
            let s3 = s3_clone.borrow_mut().lock().unwrap();
            if s1.logs.len() < 2 {
                return false;
            }
            let unified_commit_index = (s1.commit_index == s2.commit_index) && (s1.commit_index == s3.commit_index);
            let unified_logs = s1.logs.eq(&s2.logs) && s1.logs.eq(&s3.logs);
            return unified_commit_index && unified_logs;
        }, 1000).await;
        let leader_host = leader_host(s1.clone());
        let mut leader_state = states_map.get(leader_host.as_str()).unwrap();
        eventually_assert(move || {
            let state = leader_state.borrow_mut().lock().unwrap();
            return state.next_indexes.eq(vec![2, 2].as_slice());
        }, 1000).await;

        // Leader Failure
        let mut leader_state = states_map.get(leader_host.as_str()).unwrap();
        // Kill the Leader
        {
            let mut state = leader_state.borrow_mut().lock().unwrap();
            state.current_role = Dead;
        }
        let mut states = states_map.iter()
            .filter(|(k, _)| (**k).ne(leader_host.as_str()))
            .map(|(_, v)| v)
            .collect::<Vec<&Arc<Mutex<RaftConsensusState>>>>();
        let arbitrary_state = states[0].clone();
        // Check if a new Leader is elected
        eventually_assert(move || {
            let s1 = states[0].borrow_mut().lock().unwrap();
            let s2 = states[1].borrow_mut().lock().unwrap();
            if (s1.current_leader_id == leader_host) || (s2.current_leader_id == leader_host) {
                return false;
            }
            return (s1.current_role == Leader && s2.current_role == Follower)
                || (s1.current_role == Follower && s2.current_role == Leader);
        }, 1000).await;
        let res = submit_command(arbitrary_state.clone(), 3).await;
        assert_eq!(res.get_ref().success, true);

        // Old Leader Recovery
        {
            let mut state = leader_state.borrow_mut().lock().unwrap();
            // Set a non-committed log on the old leader
            let current_term = state.current_term;
            state.logs.push(LogEntry {
                term: current_term,
                payload: (33 as u64).to_le_bytes().to_vec(),
            });
            // Recover the old leader
            state.current_role = Leader;
        }
        // Check if the old leader will turn to Follower
        eventually_assert(move || {
            let state = leader_state.borrow_mut().lock().unwrap();
            return state.current_role == Follower;
        }, 1000).await;

        let (mut s1_clone, mut s2_clone, mut s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        eventually_assert(move || {
            let s1 = s1_clone.borrow_mut().lock().unwrap();
            let s2 = s2_clone.borrow_mut().lock().unwrap();
            let s3 = s3_clone.borrow_mut().lock().unwrap();
            let unified_log_size = (s1.logs.len() == s2.logs.len()) && (s1.logs.len() == s3.logs.len());
            return unified_log_size && (s1.logs == s2.logs) && (s1.logs == s3.logs);
        }, 1000).await;

        tx.send(()).unwrap();
        r1.await.unwrap();
        r2.await.unwrap();
        r3.await.unwrap();
    }
}