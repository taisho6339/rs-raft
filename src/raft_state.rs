use std::cmp::min;
use std::mem::size_of;

use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};

use crate::raft_state::RaftNodeRole::{Follower, Leader};
use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, CommandRequest, LogEntry, RequestVoteRequest, RequestVoteResult};
use crate::storage::{ApplyStorage, PersistentStateStorage};
use crate::util::{read_le_i64, read_le_u64};

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RaftNodeRole {
    Dead,
    Leader,
    Follower,
    Candidate,
}

pub struct RaftConsensusState<P: PersistentStateStorage, A: ApplyStorage> {
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

    // This is based on the premise which requires the RwLock on concurrent accesses
    pub storage: P,
    pub apply_storage: A,
}

fn randomized_timeout_duration(base_millis: i64) -> Duration {
    let mut rng = thread_rng();
    Duration::milliseconds(base_millis + rng.gen_range(0..=base_millis))
}

fn log_entries_to_bytes(entries: Vec<LogEntry>) -> Vec<u8> {
    let mut results = vec![];
    entries.iter().for_each(|entry| {
        let size = (size_of::<u64>() + size_of::<i64>() + entry.payload.len()) as u64;
        let mut size_bytes = size.to_le_bytes().to_vec();
        let mut term = entry.term.to_le_bytes().to_vec();
        results.append(&mut size_bytes);
        results.append(&mut term);
        results.append(&mut entry.payload.clone());
    });
    return results;
}

fn bytes_to_log_entries(bytes: Vec<u8>) -> Vec<LogEntry> {
    let mut results = vec![];
    if bytes.len() == 0 {
        return results;
    }
    let mut start_offset = 0 as u64;
    let bytes = bytes.as_slice();
    loop {
        let start = start_offset as usize;
        let size = read_le_u64(&mut bytes[start..(start + 8)].as_ref());
        let term = read_le_i64(&mut bytes[(start + 8)..(start + 16)].as_ref());
        let payload = bytes[(start + 16)..(start + size as usize)].to_vec();
        results.push(LogEntry {
            term,
            payload,
        });
        start_offset += size;
        if start_offset >= bytes.len() as u64 {
            break;
        }
    }
    results
}

impl<P: PersistentStateStorage, A: ApplyStorage> Default for RaftConsensusState<P, A> {
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
        let storage: P = P::new();
        let apply_storage: A = A::new();

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
            storage,
            apply_storage,
        }
    }
}

impl<P: PersistentStateStorage, A: ApplyStorage> RaftConsensusState<P, A> {
    pub(crate) fn new(
        other_peers_size: usize,
        storage: P,
        apply_storage: A,
    ) -> Self {
        let mut state = RaftConsensusState::default();
        state.initialize_indexes(other_peers_size);
        state.apply_storage = apply_storage;
        state.storage = storage;
        if state.storage.has_data() {
            state.restore_state_from_persistent_storage();
        }
        return state;
    }

    pub(crate) fn initialize_indexes(&mut self, other_peers_size: usize) {
        let length = self.logs.len() as i64;
        for _ in 0..other_peers_size {
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
        self.match_indexes = self.match_indexes.iter().map(|_| -1).collect();
        self.commit_index = -1;
        self.last_applied = -1;
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

    pub fn apply_committed_logs(&mut self) {
        if self.commit_index <= self.last_applied {
            return;
        }
        let start = self.last_applied;
        for i in (start + 1)..=self.commit_index {
            let payload = &self.logs[i as usize];
            let result = self.apply_storage.apply(payload.payload.clone());
            if result.is_err() {
                return;
            }
            self.last_applied += 1;
        }
    }

    fn restore_state_from_persistent_storage(&mut self) {
        let term_bytes = self.storage.get(String::from("current_term"));
        if let Some(term_bytes) = term_bytes {
            self.current_term = read_le_i64(&mut term_bytes.as_slice());
        }
        let voted_for_bytes = self.storage.get(String::from("voted_for"));
        if let Some(voted_for_bytes) = voted_for_bytes {
            self.voted_for = String::from_utf8(voted_for_bytes.clone()).unwrap();
        }
        let log_bytes = self.storage.get(String::from("logs"));
        if let Some(log_bytes) = log_bytes {
            self.logs = bytes_to_log_entries(log_bytes.to_vec());
        }
    }

    pub(crate) fn save_state_to_persistent_storage(&mut self) {
        let term = self.current_term.to_le_bytes().to_vec();
        let voted_for = self.voted_for.clone().into_bytes();
        let log_entries = log_entries_to_bytes(self.logs.clone());
        let _ = self.storage.set(String::from("current_term"), term);
        let _ = self.storage.set(String::from("voted_for"), voted_for);
        let _ = self.storage.set(String::from("logs"), log_entries);
    }

    pub(crate) fn apply_heartbeat_result(&mut self, result: AppendEntriesResult) {
        if result.term > self.current_term {
            self.become_follower(result.term);
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
            self.save_state_to_persistent_storage();
        }
        let last_log_index = self.last_index();
        let last_log_term = self.last_log_term();
        let acceptable = req.term == self.current_term &&
            (req.candidate_id == self.voted_for || self.voted_for == "") &&
            (req.last_log_term > last_log_term || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index));

        return if acceptable {
            self.voted_for = req.candidate_id.clone();
            self.save_state_to_persistent_storage();
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
            self.commit_index = min(req.leader_commit_index, self.last_index());
        }

        // heartbeat
        if req.logs.len() == 0 {
            return true;
        };

        // append entries
        if req.prev_log_index < 0 {
            self.logs = req.logs.clone();
            return true;
        }
        let last_log_index = self.last_index();
        if last_log_index < req.prev_log_index {
            return false;
        }
        let prev_log = self.logs[req.prev_log_index as usize].clone();
        if prev_log.term != req.prev_log_term {
            return false;
        }
        // Overwrite
        self.logs = self.logs[..=(req.prev_log_index as usize)].to_vec();
        self.logs.append(&mut req.logs.clone());
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

#[cfg(test)]
mod tests {
    use crate::{MockInMemoryKeyValueStore, MockInMemoryStorage, RaftConsensusState};
    use crate::raft_state::RaftNodeRole::{Candidate, Follower, Leader};
    use crate::rsraft::{AppendEntriesRequest, AppendEntriesResult, LogEntry, RequestVoteRequest, RequestVoteResult};
    use crate::util::read_le_u64;

    #[test]
    fn test_initialize_indexes() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        assert_eq!(state.match_indexes, vec![-1, -1]);
        assert_eq!(state.next_indexes, vec![0, 0]);
    }

    #[test]
    fn test_last_log() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_follower(1);
        assert_eq!(state.last_log_term(), -1);
        assert_eq!(state.last_index(), -1);
        let success = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: -1,
            prev_log_term: -1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (10 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(success, true);
        assert_eq!(state.last_log_term(), 1);
        assert_eq!(state.last_index(), 0);
    }

    #[test]
    fn test_become_follower() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_follower(1);
        assert_eq!(state.current_term, 1);
        assert_eq!(state.current_role, Follower);
        assert_eq!(state.current_leader_id, String::from(""));
        assert_eq!(state.voted_for, String::from(""));
        assert_eq!(state.received_granted, 0);
        assert_eq!(state.logs, vec![]);
        assert_eq!(state.commit_index, -1);
        assert_eq!(state.last_applied, -1);
        assert_eq!(state.next_indexes, vec![0, 0]);
        assert_eq!(state.match_indexes, vec![-1, -1]);
    }

    #[test]
    fn test_become_candidate() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_candidate(String::from("candidate"));
        assert_eq!(state.current_term, 1);
        assert_eq!(state.current_role, Candidate);
        assert_eq!(state.current_leader_id, String::from(""));
        assert_eq!(state.voted_for, String::from("candidate"));
        assert_eq!(state.received_granted, 1);
        assert_eq!(state.logs, vec![]);
        assert_eq!(state.commit_index, -1);
        assert_eq!(state.last_applied, -1);
        assert_eq!(state.next_indexes, vec![0, 0]);
        assert_eq!(state.match_indexes, vec![-1, -1]);
    }

    #[test]
    fn test_become_leader() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.next_indexes = vec![2, 2];
        state.match_indexes = vec![1, 1];
        state.become_leader(String::from("leader"));
        assert_eq!(state.current_term, 0);
        assert_eq!(state.current_role, Leader);
        assert_eq!(state.current_leader_id, String::from("leader"));
        assert_eq!(state.voted_for, String::from(""));
        assert_eq!(state.received_granted, 0);
        assert_eq!(state.logs, vec![]);
        assert_eq!(state.commit_index, -1);
        assert_eq!(state.last_applied, -1);
        assert_eq!(state.next_indexes, vec![0, 0]);
        assert_eq!(state.match_indexes, vec![-1, -1]);
    }

    #[test]
    fn test_update_commit_index() {
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.logs.push(LogEntry {
            term: 1,
            payload: (100 as u64).to_le_bytes().to_vec(),
        });
        state.logs.push(LogEntry {
            term: 1,
            payload: (200 as u64).to_le_bytes().to_vec(),
        });
        state.logs.push(LogEntry {
            term: 1,
            payload: (300 as u64).to_le_bytes().to_vec(),
        });
        state.match_indexes = vec![-1, -1];
        state.update_commit_index();
        assert_eq!(state.commit_index, -1);

        state.match_indexes = vec![0, 0];
        state.update_commit_index();
        assert_eq!(state.commit_index, 0);

        state.match_indexes = vec![1, 0];
        state.update_commit_index();
        assert_eq!(state.commit_index, 1);

        state.match_indexes = vec![2, 0];
        state.update_commit_index();
        assert_eq!(state.commit_index, 2);

        // if a calculated commit index is less than current one, no update happens
        state.match_indexes = vec![0, 0];
        state.update_commit_index();
        assert_eq!(state.commit_index, 2);
    }

    #[test]
    fn test_apply_heartbeat_result() {
        // initialize
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_leader(String::from("leader"));
        state.apply_heartbeat_result(AppendEntriesResult {
            term: 2,
            success: false,
        });
        assert_eq!(state.current_role, Follower);
        assert_eq!(state.current_term, 2);
    }

    #[test]
    fn test_request_vote_result() {
        // initialize
        let storage = MockInMemoryStorage::new();
        let apply_storage = MockInMemoryKeyValueStore::new();
        let mut state = RaftConsensusState::new(2, storage, apply_storage);
        state.become_candidate(String::from("candidate"));
        state.apply_request_vote_result(RequestVoteResult {
            term: 2,
            vote_granted: false,
        });
        assert_eq!(state.current_role, Follower);
        assert_eq!(state.current_term, 2);
        assert_eq!(state.received_granted, 0);

        state.become_candidate(String::from("candidate"));
        state.apply_request_vote_result(RequestVoteResult {
            term: 2,
            vote_granted: false,
        });
        assert_eq!(state.current_role, Candidate);
        assert_eq!(state.current_term, 3);
        assert_eq!(state.received_granted, 1);

        state.apply_request_vote_result(RequestVoteResult {
            term: 2,
            vote_granted: true,
        });
        assert_eq!(state.current_role, Candidate);
        assert_eq!(state.current_term, 3);
        assert_eq!(state.received_granted, 2);
    }

    #[test]
    fn test_persistent_storage() {
        let storage = MockInMemoryStorage::new();
        let apply_storage = MockInMemoryKeyValueStore::new();
        let mut state = RaftConsensusState::new(2, storage, apply_storage);
        state.become_follower(1);
        state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            candidate_id: String::from("candidate"),
            last_log_index: 0,
            last_log_term: 0,
        });
        state.logs.push(LogEntry {
            term: 1,
            payload: (100 as u64).to_le_bytes().to_vec(),
        });
        state.save_state_to_persistent_storage();
        let new_state = RaftConsensusState::new(2, state.storage, state.apply_storage);
        assert_eq!(new_state.current_term, 1);
        assert_eq!(new_state.voted_for, "candidate");
        assert_eq!(new_state.logs.len(), 1);
        assert_eq!(new_state.logs[0].term, 1);
        assert_eq!(new_state.logs[0].payload, (100 as u64).to_le_bytes().to_vec());
    }

    #[test]
    fn test_apply_append_entries_result() {
        // initialize
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_leader(String::from("leader"));
        state.logs.push(LogEntry {
            term: 0,
            payload: (100 as u64).to_le_bytes().to_vec(),
        });
        state.apply_append_entries_result(0, AppendEntriesResult {
            success: true,
            term: 0,
        });
        assert_eq!(state.next_indexes[0], 1);
        assert_eq!(state.match_indexes[0], 0);

        state.apply_append_entries_result(0, AppendEntriesResult {
            success: false,
            term: 0,
        });
        assert_eq!(state.next_indexes[0], 0);

        state.apply_append_entries_result(0, AppendEntriesResult {
            success: false,
            term: 1,
        });
        assert_eq!(state.current_role, Follower);
        assert_eq!(state.current_term, 1);
    }

    #[test]
    fn test_apply_request_vote_request() {
        // initialize
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.initialize_indexes(2);
        state.become_follower(1);
        state.logs.push(
            LogEntry {
                term: 0,
                payload: (150 as u64).to_le_bytes().to_vec(),
            }
        );
        state.logs.push(
            LogEntry {
                term: 1,
                payload: (100 as u64).to_le_bytes().to_vec(),
            }
        );
        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 0,
            last_log_term: -1,
            last_log_index: -1,
            candidate_id: String::from("candidate"),
        });
        assert_eq!(granted, false);

        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            last_log_term: -1,
            last_log_index: -1,
            candidate_id: String::from("candidate"),
        });
        assert_eq!(granted, false);

        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            last_log_term: 1,
            last_log_index: 0,
            candidate_id: String::from("candidate"),
        });
        assert_eq!(granted, false);

        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            last_log_term: 1,
            last_log_index: 1,
            candidate_id: String::from("candidate"),
        });
        assert_eq!(granted, true);
        assert_eq!(state.voted_for, "candidate");

        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            last_log_term: 1,
            last_log_index: 1,
            candidate_id: String::from("candidate"),
        });
        assert_eq!(granted, true);

        let granted = state.apply_request_vote_request(&RequestVoteRequest {
            term: 1,
            last_log_term: 2,
            last_log_index: 4,
            candidate_id: String::from("candidate2"),
        });
        assert_eq!(granted, false);
    }

    #[test]
    fn test_apply_append_entries_request() {
        // initialize
        let mut state = RaftConsensusState::<MockInMemoryStorage, MockInMemoryKeyValueStore>::default();
        state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: -1,
            prev_log_term: -1,
            logs: vec![],
        });
        assert_eq!(state.current_term, 1);
        assert_eq!(state.current_role, Follower);
        assert_eq!(state.current_leader_id, "leader");
        assert_eq!(state.commit_index, -1);
        assert_eq!(state.logs.len(), 0);

        // Append a command
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: -1,
            prev_log_term: -1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (100 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, true);
        assert_eq!(state.logs.len(), 1);

        // Append a command with illegal prev_log_index
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: 1,
            prev_log_term: 1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (200 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, false);
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: 0,
            prev_log_term: 2,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (200 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, false);

        // Append a command
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: 0,
            prev_log_term: 1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (200 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, true);
        assert_eq!(state.logs.len(), 2);

        // Overwrite existing commands
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: 0,
            prev_log_term: 1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (250 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, true);
        assert_eq!(state.logs.len(), 2);
        assert_eq!(state.logs[1].term, 1);
        assert_eq!(read_le_u64(&mut state.logs[0].payload.as_slice()), 100);
        assert_eq!(read_le_u64(&mut state.logs[1].payload.as_slice()), 250);

        // Fully replace
        let result = state.apply_append_entries_request(&AppendEntriesRequest {
            term: 1,
            leader_id: String::from("leader"),
            leader_commit_index: -1,
            prev_log_index: -1,
            prev_log_term: -1,
            logs: vec![
                LogEntry {
                    term: 1,
                    payload: (300 as u64).to_le_bytes().to_vec(),
                }
            ],
        });
        assert_eq!(result, true);
        assert_eq!(state.logs.len(), 1);
        assert_eq!(state.logs[0].term, 1);
        assert_eq!(read_le_u64(&mut state.logs[0].payload.as_slice()), 300);
    }
}