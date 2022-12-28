use std::future::Future;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

use crate::inmemory_storage::{MockInMemoryKeyValueStore, MockInMemoryStorage};
use crate::raft_client::RaftServiceClient;
use crate::raft_reconciler::RaftReconciler;
use crate::raft_server::{RaftServerConfig, RaftServerDaemon};
use crate::raft_state::{ClusterInfo, RaftConsensusState};
use crate::storage::{ApplyStorage, PersistentStateStorage};

mod raft_state;
mod raft_reconciler;
mod raft_server;
mod raft_client;
mod rsraft;
mod util;
mod storage;
mod inmemory_storage;

// pub mod rsraft {
//     tonic::include_proto!("rsraft"); // The string specified here must match the proto package name
// }

async fn run_process<F: Future<Output=()>, P: PersistentStateStorage, A: ApplyStorage>(
    close_signal: F,
    node_id: String,
    other_hosts: Vec<&'static str>,
    server_config: RaftServerConfig,
    raft_state: Arc<RwLock<RaftConsensusState<P, A>>>,
) -> Result<()> {
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
        let mut state = raft_state.write().unwrap();
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
    let other_hosts = vec![];
    let storage = MockInMemoryStorage::new();
    let apply_storage = MockInMemoryKeyValueStore::new();
    let state = RaftConsensusState::new(0, storage, apply_storage);
    let raft_state = Arc::new(RwLock::new(state));
    run_process(signal, String::from(this_node_id), other_hosts, config, raft_state).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    use ntest::timeout;
    use tokio::sync::watch::channel;
    use tokio::time::interval;
    use tonic::Response;

    use crate::raft_state::RaftNodeRole::{Dead, Follower, Leader};
    use crate::rsraft::{CommandRequest, CommandResult, LogEntry};
    use crate::rsraft::raft_client::RaftClient;
    use crate::storage::{ApplyStorage, PersistentStateStorage};
    use crate::util::convert_to_payload;

    use super::*;

    fn leader_host<P: PersistentStateStorage, A: ApplyStorage>(s1: Arc<RwLock<RaftConsensusState<P, A>>>) -> String {
        let state = s1.read().unwrap();
        state.current_leader_id.clone()
    }

    async fn submit_command<P: PersistentStateStorage, A: ApplyStorage>(s1: Arc<RwLock<RaftConsensusState<P, A>>>, key: String, payload: u64) -> Response<CommandResult> {
        let leader_host = format!("http://{}", leader_host(s1.clone()));
        let mut client = RaftClient::connect(leader_host).await.unwrap();
        let payload = convert_to_payload(key, payload);
        let req = tonic::Request::new(CommandRequest {
            payload,
        });
        return client.command(req).await.unwrap();
    }

    // fn read_command(payload: Vec<u8>) -> (String, u64) {
    //     return read_payload(payload.as_slice());
    // }

    async fn eventually_assert<F>(mut f: F, timout_millis: u64) where
        F: FnMut() -> bool {
        let flag = Arc::new(RwLock::new(false));
        let flag_clone = flag.clone();
        thread::spawn(move || {
            sleep(Duration::from_millis(timout_millis));
            let mut flag = flag_clone.write().unwrap();
            *flag = true;
        });

        let mut loop_interval = interval(Duration::from_millis(10));
        loop {
            loop_interval.tick().await;
            let success = f();
            if success {
                return;
            }
            let flag = flag.read().unwrap();
            if *flag {
                panic!("timeout");
            }
        }
    }

    async fn assert_single_leader_elected(
        s1: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s2: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s3: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
    ) {
        println!("[CASE] Make sure the single leader is elected");
        let (s1_clone, s2_clone, s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        eventually_assert(move || {
            let s1 = s1_clone.read().unwrap();
            let s2 = s2_clone.read().unwrap();
            let s3 = s3_clone.read().unwrap();
            if s1.current_leader_id == "" {
                return false;
            }
            let unified_leader = (s1.current_leader_id == s2.current_leader_id) && (s1.current_leader_id == s3.current_leader_id);
            let unified_term = (s1.current_term == s2.current_term) && (s1.current_term == s3.current_term);
            return unified_leader && unified_term;
        }, 5000).await;
    }

    async fn assert_command_replicated(
        s1: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s2: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s3: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        log_size: usize,
    ) {
        println!("[CASE] Make sure command replicated");
        let (s1_clone, s2_clone, s3_clone) = (s1.clone(), s2.clone(), s3.clone());
        eventually_assert(move || {
            let s1 = s1_clone.read().unwrap();
            let s2 = s2_clone.read().unwrap();
            let s3 = s3_clone.read().unwrap();
            if s1.logs.len() < log_size {
                return false;
            }
            let equals_logs = (s1.logs == s2.logs) && (s1.logs == s3.logs);
            let equals_commit_logs = (s1.commit_index == s2.commit_index) && (s1.commit_index == s3.commit_index);
            return equals_logs && equals_commit_logs;
        }, 5000).await;
    }

    fn kill_current_leader(leader_state: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>) {
        let mut state = leader_state.write().unwrap();
        state.current_role = Dead;
    }

    fn recover_old_leader(old_leader_state: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>) {
        let mut state = old_leader_state.write().unwrap();
        state.current_role = Leader;
    }

    async fn assert_new_member_elected_if_leader_failure(
        leader_host: String,
        other_members_states: Vec<&Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>>,
    ) {
        eventually_assert(move || {
            let s1 = other_members_states[0].read().unwrap();
            let s2 = other_members_states[1].read().unwrap();
            if (s1.current_leader_id == leader_host) || (s2.current_leader_id == leader_host) {
                return false;
            }
            return (s1.current_role == Leader && s2.current_role == Follower)
                || (s1.current_role == Follower && s2.current_role == Leader);
        }, 5000).await;
    }

    fn set_non_committed_log_to_old_leader(old_leader_state: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>) {
        let mut state = old_leader_state.write().unwrap();
        // Set a non-committed log on the old leader
        let current_term = state.current_term;
        state.logs.push(LogEntry {
            term: current_term,
            payload: convert_to_payload(String::from("third"), 33),
        });
    }

    async fn assert_old_leader_turn_follower(old_leader_state: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>) {
        println!("[CASE] The old leader turns to Follower when the recovery");
        eventually_assert(move || {
            let state = old_leader_state.read().unwrap();
            return state.current_role == Follower;
        }, 5000).await;
    }

    async fn assert_commands_applied(
        s1: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s2: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
        s3: Arc<RwLock<RaftConsensusState<MockInMemoryStorage, MockInMemoryKeyValueStore>>>,
    ) {
        eventually_assert(move || {
            let s1 = s1.read().unwrap();
            let s2 = s2.read().unwrap();
            let s3 = s3.read().unwrap();
            let s1 = &s1.apply_storage.data;
            let s2 = &s2.apply_storage.data;
            let s3 = &s3.apply_storage.data;
            let keys = vec!["first", "second", "third"];

            for k in keys.iter() {
                let r1 = s1.get(*k);
                let r2 = s2.get(*k);
                let r3 = s3.get(*k);
                if r1.is_none() || r2.is_none() || r3.is_none() {
                    return false;
                }
                if (r1.unwrap() != r2.unwrap()) || (r1.unwrap() != r3.unwrap()) {
                    return false;
                }
            }
            return true;
        }, 1000).await;
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_e2e() {
        // Initialize
        let (tx, rx) = channel(());
        let mut rx1_clone = rx.clone();
        let mut rx2_clone = rx.clone();
        let mut rx3_clone = rx.clone();
        let s1 = Arc::new(RwLock::new(RaftConsensusState::new(2, MockInMemoryStorage::new(), MockInMemoryKeyValueStore::new())));
        let s2 = Arc::new(RwLock::new(RaftConsensusState::new(2, MockInMemoryStorage::new(), MockInMemoryKeyValueStore::new())));
        let s3 = Arc::new(RwLock::new(RaftConsensusState::new(2, MockInMemoryStorage::new(), MockInMemoryKeyValueStore::new())));
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

        assert_single_leader_elected(s1.clone(), s2.clone(), s3.clone()).await;

        // Send first command
        let res = submit_command(s1.clone(), String::from("first"), 1).await;
        assert_eq!(res.get_ref().success, true);
        assert_command_replicated(s1.clone(), s2.clone(), s3.clone(), 1).await;

        // Send second command
        let res = submit_command(s1.clone(), String::from("second"), 2).await;
        assert_eq!(res.get_ref().success, true);
        assert_command_replicated(s1.clone(), s2.clone(), s3.clone(), 2).await;

        // Leader Failure
        let leader_host = leader_host(s1.clone());
        let leader_state = states_map.get(leader_host.as_str()).unwrap();
        kill_current_leader(leader_state.clone());
        let other_members_states = states_map.iter()
            .filter(|(k, _)| (**k).ne(leader_host.as_str()))
            .map(|(_, v)| v)
            .collect::<Vec<&Arc<RwLock<RaftConsensusState<_, _>>>>>();
        assert_new_member_elected_if_leader_failure(leader_host, other_members_states.clone()).await;

        let res = submit_command(other_members_states[0].clone(), String::from("third"), 3).await;
        assert_eq!(res.get_ref().success, true);

        // Old Leader Recovery
        set_non_committed_log_to_old_leader(leader_state.clone());
        recover_old_leader(leader_state.clone());
        assert_old_leader_turn_follower(leader_state.clone()).await;
        assert_command_replicated(s1.clone(), s2.clone(), s3.clone(), 3).await;

        // Apply check
        assert_commands_applied(s1.clone(), s2.clone(), s3.clone()).await;

        tx.send(()).unwrap();
        r1.await.unwrap();
        r2.await.unwrap();
        r3.await.unwrap();
    }
}