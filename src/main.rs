use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

use crate::raft::{RaftConsensusState, RaftReconciler};
use crate::server::{RaftServerConfig, RaftServerDaemon};

mod raft;
mod server;
mod client;

pub mod rsraft {
    tonic::include_proto!("rsraft"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut sig_term = signal(SignalKind::terminate()).context("failed to setup SIGTERM channel")?;
    let mut sig_int = signal(SignalKind::interrupt()).context("failed to setup SIGINT channel")?;
    let (tx, rx) = watch::channel(());
    let rx1 = rx.clone();
    let rx2 = rx.clone();
    let mut raft_state = Arc::new(Mutex::new(RaftConsensusState::default()));
    let mut raft_state1 = raft_state.clone();
    let mut raft_state2 = raft_state.clone();

    let config = RaftServerConfig {
        port: 8080,
    };
    let mut server = RaftServerDaemon::new(config);
    let s = tokio::spawn(async move {
        let _ = server.start_server(rx1, raft_state1).await;
    });

    let mut reconciler = RaftReconciler::new(raft_state2, rx2);
    let r = tokio::spawn(async move {
        let _ = reconciler.reconcile_loop().await;
    });

    {
        let mut state = raft_state.borrow_mut().lock().unwrap();
        state.become_follower(0);
    }

    select! {
        _ = sig_term.recv() => {
            println!("[INFO] Received SIGTERM");
            tx.send(()).context("failed to send SIGTERM channel")?;
        }
        _ = sig_int.recv() => {
            println!("[INFO] Received SIGINT");
            tx.send(()).context("failed to send SIGINT channel")?;
        }
    }
    s.await.unwrap();

    Ok(())
}
