use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use crate::raft_state::RaftNodeRole::{Follower, Leader};

use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, CommandRequest, LogEntry, RequestVoteRequest, RequestVoteResult};

const ELECTION_TIME_OUT_BASE_MILLIS: i64 = 150;

pub struct ClusterInfo {
    pub node_id: String,
    pub other_hosts: Vec<&'static str>,
}

impl ClusterInfo {
    pub fn new(node_id: String, hosts: Vec<&'static str>) -> Self {
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

    pub(crate) fn last_log_term(&self) -> i64 {
        let last_index = self.last_index();
        if last_index < 0 {
            return -1;
        }
        self.logs[last_index as usize].term
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

    pub(crate) fn become_candidate(&mut self, node_id: String) {
        self.voted_for = node_id;
        self.current_term += 1;
        self.current_role = RaftNodeRole::Candidate;
        self.received_granted = 1;
        self.last_heartbeat_time = Utc::now();
        self.election_timeout = randomized_timeout_duration(ELECTION_TIME_OUT_BASE_MILLIS);
    }

    pub(crate) fn become_leader(&mut self, leader_id: String) {
        self.voted_for = String::from("");
        self.received_granted = 0;
        self.current_role = RaftNodeRole::Leader;
        self.current_leader_id = leader_id;
        self.next_indexes = self.next_indexes.iter().map(|_| self.logs.len() as i64).collect::<Vec<i64>>();
        self.match_indexes = self.match_indexes.iter().map(|_| 0).collect();
    }

    pub(crate) fn update_commit_index(&mut self) {
        let mut indexes = self.match_indexes.clone();
        indexes.push(self.last_index());
        indexes.sort_by(|a, b| a.cmp(b));
        let mid_index = indexes.len() / 2;
        if self.commit_index < indexes[mid_index] {
            self.commit_index = indexes[mid_index];
            println!("[INFO] commit index updated by {}", self.commit_index);
        }
    }

    pub(crate) fn apply_heartbeat_result(&mut self, result: AppendEntriesResult) {
        if result.term > self.current_term {
            self.become_follower(result.term);
            return;
        }
    }

    pub(crate) fn apply_request_vote_result(&mut self, result: RequestVoteResult) {
        if result.term > self.current_term {
            self.become_follower(result.term);
            return;
        }
        if !result.vote_granted {
            return;
        }
        self.received_granted += 1;
    }

    pub(crate) fn apply_append_entries_result(&mut self, peer_index: usize, result: AppendEntriesResult) {
        if self.current_term < result.term {
            self.become_follower(result.term);
            return;
        }
        if result.success {
            self.next_indexes[peer_index] = self.logs.len() as i64;
            self.match_indexes[peer_index] = (self.logs.len() - 1) as i64;
        } else {
            self.next_indexes[peer_index] -= 1;
        }
    }

    pub(crate) fn apply_request_vote_request(&mut self, req: &RequestVoteRequest) -> bool {
        if req.term > self.current_term {
            self.become_follower(req.term);
        }
        let last_log_index = self.last_index();
        let last_log_term = self.last_log_term();
        let acceptable = req.term == self.current_term &&
            (req.candidate_id == self.voted_for || self.voted_for == "") &&
            (req.last_log_term > last_log_term || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index));

        return if acceptable {
            self.voted_for = req.candidate_id.clone();
            true
        } else {
            false
        };
    }

    pub(crate) fn apply_append_entries_request(&mut self, req: &AppendEntriesRequest) -> bool {
        if self.current_term > req.term {
            return false;
        }
        if self.current_role != Follower || self.current_term < req.term {
            self.become_follower(req.term);
        }

        self.last_heartbeat_time = Utc::now();
        self.current_leader_id = req.leader_id.clone();
        if self.commit_index < req.leader_commit_index {
            self.commit_index = req.leader_commit_index;
        }

        // heartbeat
        if req.logs.len() == 0 {
            return true;
        };

        // append entries
        if req.prev_log_index < 0 {
            self.logs.extend(req.logs.clone());
            return true;
        }
        let last_log_index = self.last_index();
        if last_log_index < req.prev_log_index {
            return false;
        }
        let prev_log = self.logs[req.prev_log_index as usize].clone();
        if prev_log.term != req.term {
            return false;
        }
        // Overwrite
        self.logs[..=(req.prev_log_index as usize)].to_vec().extend(req.logs.clone());
        return true;
    }

    pub(crate) fn apply_command_request(&mut self, req: &CommandRequest) -> bool {
        if self.current_role != Leader {
            return false;
        }
        let term = self.current_term;
        self.logs.push(LogEntry {
            term,
            payload: req.payload.clone(),
        });
        return true;
    }
}
