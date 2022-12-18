use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use tokio::select;
use tokio::time::interval;

// #[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
// pub struct PeerId(pub u32);

const RECONCILE_TICK_DURATION_MILLIS: u64 = 10;
const ELECTION_TIME_OUT_BASE_MILLIS: i64 = 150;

#[derive(Debug, Copy, Clone)]
pub enum RaftNodeRole {
    Dead,
    Leader,
    Follower,
    Candidate,
}

pub struct RaftConsensusState {
    current_role: Arc<Mutex<RaftNodeRole>>,
    current_term: Arc<Mutex<u64>>,
    election_timeout: Arc<Mutex<Duration>>,
    last_heartbeat_time: Arc<Mutex<DateTime<Utc>>>,
    voted_for: Arc<Mutex<i64>>,
}

fn randomized_timeout_duration(base_millis: i64) -> Duration {
    let mut rng = thread_rng();
    Duration::milliseconds(base_millis + rng.gen_range(0..=base_millis))
}

impl RaftConsensusState {
    pub fn new() -> Self {
        let current_role = Arc::new(Mutex::new(RaftNodeRole::Dead));
        let current_term = Arc::new(Mutex::new(0));
        let election_timeout = Arc::new(Mutex::new(Duration::seconds(0)));
        let last_heartbeat_time = Arc::new(Mutex::new(Utc::now()));
        let voted_for = Arc::new(Mutex::new(-1));

        RaftConsensusState {
            current_role,
            voted_for,
            current_term,
            election_timeout,
            last_heartbeat_time,
        }
    }

    fn election_timeout(&self) -> Duration {
        let t = self.election_timeout.lock().unwrap();
        *t
    }

    fn last_heartbeat_time(&self) -> DateTime<Utc> {
        let t = self.last_heartbeat_time.lock().unwrap();
        *t
    }

    fn current_role(&self) -> RaftNodeRole {
        let r = self.current_role.lock().unwrap();
        *r
    }

    fn become_follower(&mut self, term: u64) {
        *self.voted_for.lock().unwrap() = -1;
        *self.current_term.lock().unwrap() = term;
        *self.current_role.lock().unwrap() = RaftNodeRole::Follower;
        *self.last_heartbeat_time.lock().unwrap() = Utc::now();
        *self.election_timeout.lock().unwrap() = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    fn become_candidate(&mut self) {
        // self.voted_for = sel
        *self.current_term.lock().unwrap() += 1;
        *self.current_role.lock().unwrap() = RaftNodeRole::Candidate;
        *self.last_heartbeat_time.lock().unwrap() = Utc::now();
        *self.election_timeout.lock().unwrap() = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    fn reconcile_forwarder(&mut self) {
        let timeout = self.election_timeout();
        let last_heartbeat_time = self.last_heartbeat_time();

        let now = Utc::now();
        let duration = now.timestamp_millis() - last_heartbeat_time.timestamp_millis();
        if duration > timeout.num_milliseconds() {
            println!("Become Candidate");
            self.become_candidate();
        }
    }

    pub async fn reconcile_loop(&mut self) {
        let mut interval = interval(core::time::Duration::from_millis(RECONCILE_TICK_DURATION_MILLIS));

        loop {
            interval.tick().await;
            let role = self.current_role();
            match role {
                RaftNodeRole::Dead => {
                    return;
                }
                RaftNodeRole::Leader => {
                    return;
                }
                RaftNodeRole::Follower => {
                    println!("Reconcile Follower");
                    self.reconcile_forwarder();
                }
                RaftNodeRole::Candidate => {
                    println!("Reconcile Candidate");
                    return;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut rs = RaftConsensusState::new();
    rs.become_follower(0);
    println!("Start reconcile");
    rs.reconcile_loop().await;
}
