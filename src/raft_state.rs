use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};

use crate::rsraft::LogEntry;

const ELECTION_TIME_OUT_BASE_MILLIS: i64 = 150;

pub struct ClusterInfo {
    pub node_id: &'static str,
    pub other_hosts: Vec<&'static str>,
}

impl ClusterInfo {
    pub fn new(node_id: &'static str, hosts: Vec<&'static str>) -> Self {
        Self {
            node_id,
            other_hosts: hosts,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum RaftNodeRole {
    Dead,
    Leader,
    Follower,
    Candidate,
}

pub struct RaftConsensusState {
    pub current_role: RaftNodeRole,
    pub current_term: i64,

    pub current_leader_id: String,
    pub voted_for: String,
    pub election_timeout: Duration,
    pub last_heartbeat_time: DateTime<Utc>,
    pub received_granted: i64,

    pub logs: Vec<LogEntry>,
    pub commit_index: i64,
    pub last_applied: i64,
    pub next_indexes: Vec<i64>,
    pub match_indexes: Vec<i64>,
}

impl Default for RaftConsensusState {
    fn default() -> Self {
        let current_role = RaftNodeRole::Dead;
        let current_term = 0;
        let current_leader_id = String::from("");
        let election_timeout = Duration::seconds(0);
        let last_heartbeat_time = Utc::now();
        let voted_for = String::from("");
        let received_granted = 0;
        let last_applied = -1;
        let commit_index = -1;
        let logs = vec![];
        let next_indexes = vec![];
        let match_indexes = vec![];

        Self {
            voted_for,
            election_timeout,
            current_role,
            current_term,
            current_leader_id,
            last_heartbeat_time,
            received_granted,
            last_applied,
            commit_index,
            next_indexes,
            match_indexes,
            logs,
        }
    }
}

fn randomized_timeout_duration(base_millis: i64) -> Duration {
    let mut rng = thread_rng();
    Duration::milliseconds(base_millis + rng.gen_range(0..=base_millis))
}

impl RaftConsensusState {
    pub(crate) fn initialize_indexes(&mut self, peer_size: usize) {
        let length = self.logs.len() as i64;
        for _ in 0..peer_size {
            self.next_indexes.push(length);
            self.match_indexes.push(-1);
        }
    }

    pub(crate) fn last_index(&self) -> i64 {
        self.logs.len() as i64 - 1
    }

    pub(crate) fn become_follower(&mut self, term: i64) {
        self.voted_for = String::from("");
        self.received_granted = 0;
        self.current_term = term;
        self.current_role = RaftNodeRole::Follower;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    pub(crate) fn become_candidate(&mut self) {
        // self.voted_for = sel
        self.current_term += 1;
        self.current_role = RaftNodeRole::Candidate;
        self.received_granted = 1;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    pub(crate) fn become_leader(&mut self) {
        self.voted_for = String::from("");
        self.received_granted = 0;
        self.current_role = RaftNodeRole::Leader;
    }
}
