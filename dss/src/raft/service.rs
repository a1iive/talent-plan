// use labcodec::*;
// use prost::Message;
labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
        rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub candidateId: u64,
    #[prost(uint64, tag = "3")]
    pub lastLogIndex: u64,
    #[prost(uint64, tag = "4")]
    pub lastLogTerm: u64,
}
// impl Message for RequestVoteArgs {}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub voteGranted: bool,
}
// impl Message for RequestVoteReply {}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leaderId: u64,
    #[prost(uint64, tag = "3")]
    pub prevLogIndex: u64,
    #[prost(uint64, tag = "4")]
    pub prevLogTerm: u64,
    #[prost(bytes,tag ="5")]
    pub entries:Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub leaderCommit: u64,
}
// impl Message for AppendEntriesArgs {}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesReply {
    // Your data here (2A, 2B).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    #[prost(uint64 ,tag ="3")]
    pub matchIndex :u64,
}
// impl Message for AppendEntriesReply {}
