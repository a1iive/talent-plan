use std::sync::{Arc, Mutex ,mpsc::{channel,Sender,Receiver}};
use std::thread;
use std::time;
use futures::future;
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labcodec;
use labrpc::RpcFuture;
use rand::Rng;
use std::time::Duration;
// use config::Entry;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry{
    #[prost(uint64, tag = "1")]
    pub term :u64,
    #[prost(uint64, tag = "2")]
    pub index :u64,
    #[prost(bytes, tag = "3")]
    pub ord :Vec<u8>,
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}
#[derive(Debug)]
pub enum RaftState{
    Follower,
    Candidate,
    Leader,
}
impl Clone for RaftState {
    fn clone(&self) -> Self {
        match *self {
            RaftState::Follower => RaftState::Follower,
            RaftState::Candidate => RaftState::Candidate,
            RaftState::Leader => RaftState::Leader,
            
        }
    }

    fn clone_from(&mut self, _source: &Self) {
        
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voteFor: Option<u64>,
    log: Vec<Vec<u8>>,
    commitIndex: u64,
    lastApplied: u64,
    nextIndex: Vec<u64>,
    matchIndex: Vec<u64>,

    voteCount: u32,
    raft_state:RaftState,
    get_heartbeats:bool,
    logEnsure:Vec<u64>,
    apply_ch:UnboundedSender<ApplyMsg>,
}

impl Clone for Raft {
    fn clone(&self) -> Raft {
        Raft{
            peers:self.peers.clone(),
            persister:Box::new(SimplePersister::new()),
            me:self.me,
            state:self.state.clone(),
            voteFor:self.voteFor,
            log: self.log.clone(),
            commitIndex: self.commitIndex,
            lastApplied: self.lastApplied,
            nextIndex: self.nextIndex.clone(),
            matchIndex: self.matchIndex.clone(),
            
            voteCount: self.voteCount,
            raft_state: self.raft_state.clone(),
            get_heartbeats: self.get_heartbeats,
            logEnsure:self.logEnsure.clone(),
            apply_ch:self.apply_ch.clone(),
        }
    }

    fn clone_from(&mut self, _source: &Self) {
        
    }
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let state = State {
            term: 1,
            is_leader: false,
        };
        // println!("me:{}", me);
        let len = peers.len();
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::new(state),
            voteFor: None,
            log: Vec::new(),
            commitIndex: 0,
            lastApplied: 0,
            nextIndex: vec![1; len],
            matchIndex: vec![0; len],
            voteCount: 0,
            raft_state: RaftState::Follower,
            get_heartbeats:false,
            logEnsure:Vec::new(),
            apply_ch:apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);

        // let mut data = Vec::<u8>::new();
        // let state = Arc::clone(&self.state);
        // labcodec::encode(&state.term(), &mut data).unwrap();
        // labcodec::encode(&state.voteFor, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }

        // let state = Arc::clone(&self.state);
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         state.term = o.term;
        //         state.voteFor = o.voteFor;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns OK(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // println!("server:{} args:{:?}",server,args );
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             OK(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        // println!("vote fun ->{}:{}", self.me,server);
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Result<AppendEntriesReply> {
        let peer = &self.peers[server];
        peer.append_entries(&args).map_err(Error::Rpc).wait()
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let is_leader = self.state.is_leader();
        if is_leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).unwrap();
            // Your code here (2B).
            let ord = buf;
            let state = Arc::clone(&self.state);
            let term = state.term();
            let index: u64 = (self.log.len() +1) as u64;
            let logentry = LogEntry {
                term:term,
                index:index,
                ord : ord,
            };
            let mut buf = vec![];
            labcodec::encode(&logentry, &mut buf).unwrap();
            self.log.push(buf);
            let mut l = self.logEnsure.len();
            while l < index as usize{
                self.logEnsure.push(1);
                l +=1;
            }
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (tx,rx)= channel();
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
        };
        Node::make(Arc::clone(&node.raft),tx.clone(),rx);
        
        //heartbeats
        

        node
    }
    //start work
    pub fn make(raft:Arc<Mutex<Raft>>,tx:Sender<u32>,rx:Receiver<u32>) {
        let len = {
            let raft = raft.lock().unwrap();
            raft.peers.len()
        };
        let me = {
            let raft = raft.lock().unwrap();
            raft.me
        };
        thread::spawn(move || {
            let mut random = rand::thread_rng();
            loop{
                let raft_state:RaftState;
                {
                    let raft = Arc::clone(&raft);
                    // println!("{}:lock 1",me );
                    let mut raft = raft.lock().unwrap();
                    raft_state = raft.raft_state.clone();
                    while raft.commitIndex>raft.lastApplied {
                        raft.lastApplied +=1;
                        let log:Vec<u8> = raft.log[(raft.lastApplied-1) as usize].clone();
                        let log:LogEntry = labcodec::decode(&log).unwrap();
                        // println!("{} apply:commitIndex={} lastApplied={} log:{:?}"
                        //     ,raft.me,raft.commitIndex,raft.lastApplied,log );
                        let t = ApplyMsg {
                            command_valid: true,
                            command: log.ord.clone(),
                            command_index: log.index,
                        };   
                        raft.apply_ch.unbounded_send(t).unwrap();
                    }
                };
                // println!("{}:unlock 1",me );
                match raft_state {
                    RaftState::Leader =>{
                        // println!("in leader fork");
                        let ms: u64 = random.gen_range(50, 60);
                        thread::sleep(Duration::from_millis(ms));
                        let rf = {
                            let raft = Arc::clone(&raft);
                            // println!("{}:lock 2",me );
                            let raft = raft.lock().unwrap();
                            (*raft).clone()
                        };
                        // println!("{}:unlock 2",me );
                        let args = {
                            if rf.log.len() == 0{
                                AppendEntriesArgs {
                                        term: rf.state.term(),
                                        leaderId: rf.me as u64,
                                        prevLogIndex: 0,
                                        prevLogTerm: 0,
                                        entries:vec![],
                                        leaderCommit: 0,
                                    }
                            }
                            else {
                                let index = rf.log.len();
                                let log:Vec<u8> = rf.log[index-1].clone();
                                let log:LogEntry = labcodec::decode(&log).unwrap();
                                let prevLogTerm = log.term;
                                AppendEntriesArgs {
                                        term: rf.state.term(),
                                        leaderId: rf.me as u64,
                                        prevLogIndex: index as u64,
                                        prevLogTerm: prevLogTerm,
                                        entries:vec![],
                                        leaderCommit: rf.commitIndex,
                                    }
                            }
                            
                        };
                        for i in 0..rf.peers.len(){
                            if i!=me{
                                let index = rf.log.len();
                                if index >= rf.nextIndex[i] as usize{
                                    let nextIndex = rf.nextIndex[i];
                                    let args = {
                                        if nextIndex ==1 {
                                            AppendEntriesArgs{
                                                term:rf.state.term(),
                                                leaderId:rf.me as u64,
                                                prevLogIndex:0,
                                                prevLogTerm:0,
                                                entries:rf.log[(nextIndex-1) as usize].clone(),
                                                leaderCommit:rf.commitIndex,
                                            }
                                        }
                                        else{
                                            let log = rf.log[(nextIndex-2) as usize].clone();
                                            let log:LogEntry = labcodec::decode(&log).unwrap();
                                            AppendEntriesArgs{
                                                term:rf.state.term(),
                                                leaderId:rf.me as u64,
                                                prevLogIndex:log.index,
                                                prevLogTerm:log.term,
                                                entries:rf.log[(nextIndex-1) as usize].clone(),
                                                leaderCommit:rf.commitIndex,
                                            }
                                        }
                                    };     
                                    // println!("{}:loglen={} nextIndex[{}]={}",me,index,i, rf.nextIndex[i]);
                                    // println!("{}:index={} nextIndex[{}]={}",me,nextIndex,i, rf.nextIndex[i]);
                                    Node::append(Arc::clone(&raft), i, nextIndex as usize, &args);
                                }
                                else {
                                    Node::heart(Arc::clone(&raft),i, &args);
                                }
                            }
                        }                        
                    }
                    RaftState::Follower =>{
                        let ms: u64 = random.gen_range(300, 500);
                        thread::sleep(Duration::from_millis(ms));
                        {
                            let raft = Arc::clone(&raft);
                            // println!("{}:lock 3",me );
                            let mut raft = raft.lock().unwrap();
                            // println!("{}:if get_heartbeats", me);
                            if raft.get_heartbeats == true{
                                raft.get_heartbeats = false;
                                // println!("{}:break", me);
                                continue;
                            }
                            println!("{} become Candidate",raft.me );
                            raft.raft_state= RaftState::Candidate;
                        }
                        // println!("{}:unlock 3",me );
                    }
                    RaftState::Candidate =>{
                        {
                            // println!("{}:lock 4",me );
                            let mut raft = raft.lock().unwrap();
                            if raft.get_heartbeats == true {
                                println!("{} true break",raft.me );
                                raft.get_heartbeats = false;
                                raft.raft_state = RaftState::Follower;
                                continue;
                            }
                        }
                        // println!("{}:unlock 4",me );
                        let args:RequestVoteArgs;
                        {
                            // println!("{}:lock 5",me );
                            let mut raft = raft.lock().unwrap();
                            raft.raft_state = RaftState::Candidate;
                            raft.voteCount = 1;
                            raft.state =Arc::new(State{
                                term:raft.state.term()+1,
                                is_leader:raft.state.is_leader(),
                            });
                            raft.voteFor = Some(raft.me as u64);
                            if raft.log.len() == 0 {
                                args = RequestVoteArgs {
                                    term: raft.state.term(),
                                    candidateId: raft.me as u64,
                                    lastLogIndex: 0,
                                    lastLogTerm: 0,
                                };
                            }
                            else {
                                let index = raft.log.len();
                                let log:Vec<u8> = raft.log[index-1].clone();
                                let log:LogEntry = labcodec::decode(&log).unwrap();
                                let lastLogTerm = log.term;
                                args = RequestVoteArgs {
                                    term: raft.state.term(),
                                    candidateId: me as u64 ,
                                    lastLogIndex: index as u64,
                                    lastLogTerm: lastLogTerm,
                                };
                            }
                        }
                        // println!("{}:unlock 5",me );
                        {
                            // println!("{}:lock 6",me );
                            let mut raft = raft.lock().unwrap();
                            if raft.get_heartbeats == true {
                                raft.get_heartbeats = false;
                                continue;
                            }
                        }
                        // println!("{}:unlock 6",me );

                        println!("{}:start vote",me);
                        Node::vote(Arc::clone(&raft),&args.clone(),tx.clone());
                        let ms: u64 = random.gen_range(300, 500);
                        match rx.recv_timeout(time::Duration::from_millis(ms)){
                            Ok(_) =>{
                                {
                                    {
                                        // println!("{}:lock 7",me );
                                        let mut raft = raft.lock().unwrap();
                                        if raft.state.is_leader == false {
                                            raft.raft_state = RaftState::Follower;
                                        }
                                        else {
                                            println!("{} change to leader", me);
                                            raft.raft_state = RaftState::Leader;
                                        }
                                    }
                                    // println!("{}:unlock 7",me );
                                }
                            }
                            Err(e) =>{
                                let mut raft = raft.lock().unwrap();
                                raft.raft_state = RaftState::Follower;
                                println!("while electing timeout:{}", e);
                            }
                        }
                    }
                }
                // println!("{}:loop over",me );
            }
        });
    }
    //start vote request
    pub fn vote(raft:Arc<Mutex<Raft>>,args:&RequestVoteArgs,tx:Sender<u32>) {
        let len = {
            let raft = raft.lock().unwrap();
            raft.peers.len()
        };
        let me = {
            let raft = raft.lock().unwrap();
            raft.me
        };
        // let (tx,rx) = mpsc::channel();
        let major = (len as u32) /2 +1;
        for i in 0..len {
            if i == me{
            }
            else{
                let args = args.clone();
                let tx = tx.clone();
                let raft = Arc::clone(&raft);
                thread::spawn(move || {
                    let rf = {
                        // println!("{}:lock 8",me );
                        let raft = raft.lock().unwrap();
                        (*raft).clone()
                    };
                    // println!("{}:unlock 8",me );
                    {
                        if rf.get_heartbeats {
                            // println!("tx end 1");
                            tx.send(1).unwrap();
                        }else{
                            // println!("{} to {} : {:?}",me,i,args );
                            let reply = rf.send_request_vote(i,&args);
                            // println!("{} from {} : {:?}",me,i,reply );
                            match reply {
                                Ok(reply)=>{
                                    let rf = {
                                        let raft = Arc::clone(&raft);
                                        // println!("{}:lock 9",me );
                                        let raft = raft.lock().unwrap();
                                        (*raft).clone()
                                    };
                                    // println!("{}:unlock 9",me );
                                    if reply.term > rf.state.term() {
                                        {
                                            let raft = Arc::clone(&raft);
                                            // println!("{}:lock 10",me );
                                            let mut raft = raft.lock().unwrap();
                                            raft.state = Arc::new(State{
                                                term:reply.term,
                                                is_leader:false,
                                            });
                                            raft.raft_state = RaftState::Follower;
                                            raft.voteCount = 0;
                                            // break;
                                        }
                                        // println!("{}:unlock 10",me );
                                    }else {
                                        if reply.voteGranted {
                                            {
                                                let rf = Arc::clone(&raft);
                                                // println!("{}:lock 11",me );
                                                let mut rf = raft.lock().unwrap();
                                                rf.voteCount += 1;
                                            }
                                            // println!("{}:unlock 11",me );
                                            let rf = {
                                                let raft = Arc::clone(&raft);
                                                // println!("{}:lock 12",me );
                                                let raft = raft.lock().unwrap();
                                                (*raft).clone()
                                            };
                                            // println!("{}:unlock 12",me );
                                            if rf.state.is_leader(){
                                            }else {
                                                if rf.get_heartbeats == false{
                                                    if rf.voteCount >= major {
                                                        {
                                                            let rf = Arc::clone(&raft);
                                                            // println!("{}:lock 13",me );
                                                            let mut rf = raft.lock().unwrap();
                                                            rf.state = Arc::new(State{
                                                                term:rf.state.term(),
                                                                is_leader:true,
                                                            });
                                                            rf.raft_state = RaftState::Leader;
                                                            let len = rf.log.len();
                                                            let size = rf.peers.len();
                                                            rf.nextIndex = vec![(len+1) as u64;size];
                                                            rf.matchIndex = vec![0;size];
                                                        }
                                                        // println!("{}:unlock 13",me );
                                                        let args = {
                                                            if rf.log.len() == 0 {
                                                                AppendEntriesArgs {
                                                                    term: rf.state.term(),
                                                                    leaderId: rf.me as u64,
                                                                    prevLogIndex: 0,
                                                                    prevLogTerm: 0,
                                                                    entries:vec![],
                                                                    leaderCommit: 0,
                                                                }
                                                            }
                                                            else{
                                                                
                                                                let index = rf.log.len();
                                                                let log:Vec<u8> = rf.log[index-1].clone();
                                                                let log:LogEntry = labcodec::decode(&log).unwrap();
                                                                let prevLogTerm = log.term;
                                                                AppendEntriesArgs {
                                                                    term: rf.state.term(),
                                                                    leaderId: rf.me as u64,
                                                                    prevLogIndex: index as u64,
                                                                    prevLogTerm: prevLogTerm,
                                                                    entries:vec![],
                                                                    leaderCommit: rf.commitIndex,
                                                                }
                                                            }
                                                            
                                                        };
                                                        {
                                                            let rf = Arc::clone(&raft);
                                                            // println!("{}:lock 14",me );
                                                            let rf = raft.lock().unwrap();
                                                            println!("{} become leader",rf.me);                                                        
                                                        }   
                                                        // println!("{}:unlock 14",me );
                                                        // println!("tx end 2");
                                                        tx.send(1).unwrap();                                                   
                                                        for i in 0..rf.peers.len() {
                                                            if i!=rf.me{
                                                                Node::heart(Arc::clone(&raft),i,&args.clone());
                                                            }
                                                        }                                                        
                                                    }
                                                }
                                                else {
                                                    // println!("tx end 3");
                                                    tx.send(1).unwrap();
                                                }
                                            } 
                                        } 
                                    }
                                }
                                Err(_) =>{}
                            }
                            // tx.send(reply).unwrap();
                        } 
                    }
                    
                });
            }
        }
        
        
        // for i in 0..len-1{
        //     println!("s:{}",i );
        //     {
        //         let raft = Arc::clone(&raft);
        //         let raft = raft.lock().unwrap();
        //         if raft.get_heartbeats {break;}
        //     }
        //     match rx.recv().unwrap() {
        //         Ok(reply)=>{
                    
        //             let rf = {
        //                 let raft = Arc::clone(&raft);
        //                 let raft = raft.lock().unwrap();
        //                 (*raft).clone()
        //             };
        //             if reply.term > rf.state.term() {
        //                 let raft = Arc::clone(&raft);
        //                 let mut raft = raft.lock().unwrap();
        //                 raft.state = Arc::new(State{
        //                     term:reply.term,
        //                     is_leader:false,
        //                 });
        //                 raft.raft_state = RaftState::Follower;
        //                 raft.voteCount = 0;
        //                 break;
        //             }else {
        //                 if reply.voteGranted {
        //                     {
        //                         let rf = Arc::clone(&raft);
        //                         let mut rf = raft.lock().unwrap();
        //                         rf.voteCount += 1;
        //                     }
        //                     let rf = {
        //                         let raft = Arc::clone(&raft);
        //                         let raft = raft.lock().unwrap();
        //                         (*raft).clone()
        //                     };
        //                     if rf.state.is_leader(){
        //                     }else {
        //                         if rf.get_heartbeats {break;}
        //                         if rf.voteCount >= major {
        //                             {
        //                                 let rf = Arc::clone(&raft);
        //                                 let mut rf = raft.lock().unwrap();
        //                                 rf.state = Arc::new(State{
        //                                     term:rf.state.term(),
        //                                     is_leader:true,
        //                                 });
        //                                 rf.raft_state = RaftState::Leader;
        //                                 let len = rf.log.len();
        //                                 rf.nextIndex = vec![(len+1) as u64;len];
        //                                 rf.matchIndex = vec![0;len];
        //                             }
        //                             let args = {
        //                                 if rf.log.len() == 0 {
        //                                     AppendEntriesArgs {
        //                                         term: rf.state.term(),
        //                                         leaderId: rf.me as u64,
        //                                         prevLogIndex: 0,
        //                                         prevLogTerm: 0,
        //                                         entries:vec![],
        //                                         leaderCommit: 0,
        //                                     }
        //                                 }
        //                                 else{
                                            
        //                                     let index = rf.log.len();
        //                                     let log:Vec<u8> = rf.log[index-1].clone();
        //                                     let log:LogEntry = labcodec::decode(&log).unwrap();
        //                                     let prevLogTerm = log.term;
        //                                     AppendEntriesArgs {
        //                                         term: rf.state.term(),
        //                                         leaderId: rf.me as u64,
        //                                         prevLogIndex: index as u64,
        //                                         prevLogTerm: prevLogTerm,
        //                                         entries:vec![],
        //                                         leaderCommit: rf.commitIndex,
        //                                     }
        //                                 }
                                        
        //                             };
        //                             println!("{} become leader",rf.me);
                                    
        //                             Node::heart(Arc::clone(&raft),&args.clone());
        //                         }
        //                     } 
        //                 } 
        //             }
        //         }
        //         Err(_) =>{}
        //     }
        //     println!("e:{}",i );
            
        // }
    }
    //start heartbeats
    pub fn heart(raft:Arc<Mutex<Raft>>,i:usize,args:&AppendEntriesArgs) {
        let len = {
            let raft = raft.lock().unwrap();
            raft.peers.len()
        };
        let me = {
            let raft = raft.lock().unwrap();
            raft.me
        };
        // println!("{} start heart", me);
        // let (tx,rx) = mpsc::channel();
        // for i in 0..len{
        //     if i==me {continue;}
        //     else {
        let args = args.clone();
        // let tx = tx.clone();
        let raft = Arc::clone(&raft);
        
        thread::spawn(move || {
            let rf = {
                // println!("{}:lock 15",me );
                let raft = raft.lock().unwrap();
                (*raft).clone()
            };
            // println!("{}:unlock 15",me );
            if rf.state.is_leader() {
                // println!("{} to {} : {:?}",me,i,args );
                let reply = rf.send_append_entries(i,&args);
                // println!("{} from {} : {:?}",me,i,reply );
                match reply{
                    Ok(reply)=>{
                        {
                            {
                                // println!("{}:lock 16",me );
                                let mut raft = raft.lock().unwrap();
                                if reply.term > raft.state.term() {
                                    raft.state = Arc::new(State{
                                        term:reply.term,
                                        is_leader:false,
                                    });
                                    raft.raft_state = RaftState::Follower;
                                    raft.voteCount = 0;
                                    // break;
                                }
                            }
                            // println!("{}:unlock 16",me );
                        }
                    }
                    Err(_)=>{}
                }
                // tx.send(reply).unwrap();
            }
        });
        //     }
        // }

        // for i in 0..len-1 {
        //     match rx.recv().unwrap(){
        //         Ok(reply)=>{
        //             {

        //                 let mut raft = raft.lock().unwrap();
        //                 if reply.term > raft.state.term() {
        //                     raft.state = Arc::new(State{
        //                         term:reply.term,
        //                         is_leader:false,
        //                     });
        //                     raft.raft_state = RaftState::Follower;
        //                     raft.voteCount = 0;
        //                     break;
        //                 }
        //             }
        //         }
        //         Err(_)=>{}
        //     } 
        // }

        // println!("{} heart over", me);
    }
    //start append entries
    pub fn append(raft:Arc<Mutex<Raft>>,i:usize,index:usize,args:&AppendEntriesArgs){
        let len = {
            let raft = raft.lock().unwrap();
            raft.peers.len()
        };
        let me = {
            let raft = raft.lock().unwrap();
            raft.me
        };
        // println!("{}:start append log",me );
        let major = (len/2+1) as u64;
        // for i in 0..len{
        //     if i==me {continue;}
        //     else {
        let args:AppendEntriesArgs=args.clone();
        {
            let raft = raft.lock().unwrap();
            if raft.nextIndex[i] == index as u64{
                // args=args.clone()
            }
            else {
                return;
                
            }
        }
        
        // let tx = tx.clone();
        let raft = Arc::clone(&raft);
        let rf = {
            let raft = raft.lock().unwrap();
            (*raft).clone()
        };
        thread::spawn(move || {
            if rf.state.is_leader() {
                // println!("{} to {} : {:?}",me,i,args );
                let reply = rf.send_append_entries(i,&args);
                // println!("{} from {} : {:?}",me,i,reply );
                match reply{
                    Ok(reply)=>{ 
                        if reply.term > rf.state.term() {
                            let mut raft = raft.lock().unwrap();
                            raft.state = Arc::new(State{
                                term:reply.term,
                                is_leader:false,
                            });
                            raft.raft_state = RaftState::Follower;
                            raft.voteCount = 0;
                        }
                        else {
                            if reply.success {
                                let mut raft = raft.lock().unwrap();
                                raft.logEnsure[index-1] +=1;
                                if raft.logEnsure[index-1] >= major{
                                    if raft.commitIndex <= index as u64 {
                                        raft.commitIndex = index as u64;
                                    } 
                                    // println!("{}:commitIndex = {}",raft.me,raft.commitIndex );
                                    
                                }
                                // println!("logEnsure:{} reply.matchIndex:{} raft.matchIndex[{}]={}",raft.logEnsure[index-1],reply.matchIndex,i,raft.matchIndex[i]);
                                if reply.matchIndex>raft.matchIndex[i]{
                                    raft.matchIndex[i]=reply.matchIndex;
                                    raft.nextIndex[i] = raft.matchIndex[i]+1;
                                }
                            }
                            else {
                                let mut raft = raft.lock().unwrap();
                                if (raft.nextIndex[i]-1)>=1{
                                    raft.nextIndex[i] -=1;
                                }
                                if (raft.matchIndex[i]) >= 1{
                                    raft.matchIndex[i]-=1;
                                }
                            }
                            
                        }
                    }
                    Err(_)=>{}
                }
                // tx.send(reply).unwrap();
            }
        });
        //     }
        // }
        
        // for i in 0..len-1 {
        //     match rx.recv().unwrap(){
        //         Ok(reply)=>{
        //             {
        //                 let mut raft = raft.lock().unwrap();
        //                 if reply.term > raft.state.term() {
        //                     raft.state = Arc::new(State{
        //                         term:reply.term,
        //                         is_leader:false,
        //                     });
        //                     raft.raft_state = RaftState::Follower;
        //                     raft.voteCount = 0;
        //                     break;
        //                 }
        //                 else {
        //                     if reply.success {

        //                     }
        //                 }
        //             }
        //         }
        //         Err(_)=>{}
        //     }    
        // }
    }

    pub fn find_N(raft:Arc<Mutex<Raft>>) ->Option<u64>{
        let rf = {
            let raft=raft.lock().unwrap();
            (*raft).clone()
        };
        let mut ret:Option<u64> = None;
        let Index = rf.log.len() ;
        let len = rf.peers.len();
        let major = (len/2+1 )as u64;
        let mut count ;
        let me = rf.me;
        let commitIndex = rf.commitIndex as usize;
        for N in (commitIndex+1)..(Index+1){
            count = 0;
            for i in 0..len {
                if i == me{continue;}
                else {
                    if rf.matchIndex[i] >= N as u64{
                        count +=1;
                        if count >= major{
                            let log = rf.log[N-1].clone();
                            let log:LogEntry = labcodec::decode(&log).unwrap();
                            if log.term == rf.state.term(){
                                ret = Some(N as u64);
                                break;
                            }
                        }
                    }
                }
            }
            match ret{
                Some(_) =>{break;}
                None=>{continue;}
            }
        }
        ret
    }
    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    /// This method must return quickly.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        // unimplemented!()
        let raft = self.raft.clone();
        let me ={
            let raft = raft.lock().unwrap();
            raft.me
        };
        
        let ret ={
            // println!("{}:lock 17",me );
            let mut raft = raft.lock().unwrap();
            raft.start(command)
        };
        // println!("{}:unlock 17",me );

        let raft = {
            let raft = Arc::clone(&self.raft);
            // println!("{}:lock 18",me );
            let raft = raft.lock().unwrap();
            (*raft).clone()
        };
        // println!("{}:unlock 18",me );

        match ret {
            Ok((index,term)) =>{
                let args = {
                    if index == 1{
                        // first one log
                        AppendEntriesArgs{
                            term:raft.state.term(),
                            leaderId:raft.me as u64,
                            prevLogIndex:0,
                            prevLogTerm:0,
                            entries:raft.log[(index-1) as usize].clone(),
                            leaderCommit:raft.commitIndex,
                        }
                    }
                    else {
                        let prevLogIndex = index-1;
                        let log:Vec<u8> = raft.log[(prevLogIndex-1) as usize].clone();
                        let log:LogEntry = labcodec::decode(&log).unwrap();
                        AppendEntriesArgs{
                            term:raft.state.term(),
                            leaderId:raft.me as u64,
                            prevLogIndex:prevLogIndex,
                            prevLogTerm:log.term,
                            entries:raft.log[(index-1) as usize].clone(),
                            leaderCommit:raft.commitIndex,
                        }
                    }
                };

                // println!("{} invoke append:{:?}",raft.me,args );
                for i in 0..raft.peers.len(){
                    if i != me{
                        Node::append(Arc::clone(&self.raft),i,index as usize,&args); 
                    }
                }
            }
            Err(_) =>{}
        }
        ret
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        let me ={
            let raft = self.raft.clone();
            let raft = raft.lock().unwrap();
            raft.me
        };
        let state = {
            // println!("{}:lock 19",me );
            let raft=self.raft.clone();
            let raft=raft.lock().unwrap();
            raft.state.clone()
        };
        // println!("{}:unlock 19",me );
        state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        let me ={
            let raft = self.raft.clone();
            let raft = raft.lock().unwrap();
            raft.me
        };
        let state = {
            // println!("{}:lock 20",me );
            let raft = self.raft.clone();
            let raft = raft.lock().unwrap();
            raft.state.clone()
        };
        // println!("{}:unlock 20",me );
        state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        // State {
        //     term: self.term(),
        //     is_leader: self.is_leader(),
        // }
        let me ={
            let raft = self.raft.clone();
            let raft = raft.lock().unwrap();
            raft.me
        };
        let state:State = {
            let raft = self.raft.clone();
            // println!("{}:lock 21",me );
            let raft = raft.lock().unwrap();
            let state = raft.state.clone();
            State {
                term: state.term(),
                is_leader: state.is_leader(),
            }   
        };
        // println!("{}:unlock 21",me );
        state
        
        
    }

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        println!("a Raft instance has been killed");
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // unimplemented!()
        let raft = Arc::clone(&self.raft);
        let mut tag:bool =false;
        // println!("vote tag" );
        let mut raft = raft.lock().unwrap(); 
        // println!("{}:get vote",raft.me );
        if args.term < raft.state.term() {
            let reply = RequestVoteReply {
                term: raft.state.term(),
                voteGranted: false,
            };
            Box::new(future::result(Ok(reply)))
        } else {
            let commitTerm = {
                if raft.log.len() == 0 || raft.commitIndex == 0{
                    0
                }
                else {
                    let log:Vec<u8> = raft.log[(raft.commitIndex-1) as usize].clone();
                    let logs:LogEntry = labcodec::decode(&log).unwrap();
                    logs.term
                }
            };
            if args.term > raft.state.term(){
                raft.state = Arc::new(State{
                    term:args.term,
                    is_leader:false,
                });
                raft.raft_state = RaftState::Follower;
                raft.voteCount = 0;
            }
            else {
                tag = true;
            }
            match raft.voteFor {
                Some(voteFor) => {
                    if tag && voteFor !=args.candidateId {
                        // println!("return false 1");
                        let reply = RequestVoteReply {
                            term: raft.state.term(),
                            voteGranted: false,
                        };
                        Box::new(future::result(Ok(reply)))
                    }
                    else {
                    // if voteFor == args.candidateId {
                        if args.lastLogTerm > commitTerm {
                            raft.voteFor = Some(args.candidateId);
                            let reply = RequestVoteReply {
                                term: raft.state.term(),
                                voteGranted: true,
                            };
                            Box::new(future::result(Ok(reply)))
                        } else if args.lastLogTerm == commitTerm {
                            if args.lastLogIndex >= raft.commitIndex {
                                raft.voteFor = Some(args.candidateId);
                                let reply = RequestVoteReply {
                                    term: raft.state.term(),
                                    voteGranted: true,
                                };
                                Box::new(future::result(Ok(reply)))
                            } else {
                                // println!("return false 2");
                                raft.voteFor = None;
                                let reply = RequestVoteReply {
                                    term: raft.state.term(),
                                    voteGranted: false,
                                };
                                Box::new(future::result(Ok(reply)))
                            }
                        } 
                        else {
                            // println!("return false 1");
                            raft.voteFor = None;
                            let reply = RequestVoteReply {
                                term: raft.state.term(),
                                voteGranted: false,
                            };
                            Box::new(future::result(Ok(reply)))
                        }
                    }
                    // } 
                    // else {
                    //     let reply = RequestVoteReply {
                    //         term: raft.state.term(),
                    //         voteGranted: false,
                    //     };
                    //     raft.voteFor = None;
                    //     Box::new(future::result(Ok(reply)))
                    // }
                }
                None => {
                    if args.lastLogTerm > commitTerm {
                        raft.voteFor = Some(args.candidateId);
                        let reply = RequestVoteReply {
                            term: raft.state.term(),
                            voteGranted: true,
                        };
                        Box::new(future::result(Ok(reply)))
                    } else if args.lastLogTerm == commitTerm {
                        if args.lastLogIndex >= commitTerm {
                            raft.voteFor = Some(args.candidateId);
                            let reply = RequestVoteReply {
                                term: raft.state.term(),
                                voteGranted: true,
                            };
                            Box::new(future::result(Ok(reply)))
                        } else {
                            let reply = RequestVoteReply {
                                term: raft.state.term(),
                                voteGranted: false,
                            };
                            Box::new(future::result(Ok(reply)))
                        }
                    } else {
                        let reply = RequestVoteReply {
                            term: raft.state.term(),
                            voteGranted: false,
                        };
                        Box::new(future::result(Ok(reply)))
                    }
                }
            }
        }
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let rf = {
            let raft = Arc::clone(&self.raft);
            // println!("heart tag");
            let raft = raft.lock().unwrap();
            // println!("{}:get heart", raft.me);
            (*raft).clone()
        };
        // if args.entries.len()>0{
        //     println!("{}:{:?}",rf.me,args );
        // }
        if args.term < rf.state.term(){ //  (1) 判断
            let reply = AppendEntriesReply {
                term: rf.state.term(),
                success: false,
                matchIndex:0,
            };
            Box::new(future::result(Ok(reply)))
        }
        else {
            let raft = Arc::clone(&self.raft);
            {
                let mut raft = raft.lock().unwrap();
                match raft.raft_state {
                    RaftState::Follower => {
                        
                        raft.get_heartbeats = true;
                    }
                    RaftState::Candidate =>{
                        raft.raft_state = RaftState::Follower;
                        raft.get_heartbeats = true;
                    }
                    RaftState::Leader =>{
                        raft.state = Arc::new(State{
                            term:args.term,
                            is_leader:false,
                        });
                        raft.raft_state = RaftState::Follower;
                        raft.get_heartbeats = true;
                    }
                }
            }
            let raft = {
                let raft = raft.lock().unwrap();
                // println!("{}:get heart = {}", raft.me,raft.get_heartbeats);
                (*raft).clone()
            };
            let prevLogIndex = args.prevLogIndex as usize;
            let log_size = args.entries.len();
            if log_size == 0 {
                // heart beats 
                if prevLogIndex == 0{ //因为是心跳，意味着leader里无log
                     //(2) 必匹配 然后由于心跳包，不进行(3)
                    let reply = AppendEntriesReply {
                        term: raft.state.term(),
                        success: true,
                        matchIndex:0,
                    };
                    // println!("{}:tag2", raft.me);
                    Box::new(future::result(Ok(reply)))  
                }
                else {
                    let index = raft.log.len()  ;
                    if index < prevLogIndex { //意味着接受方最新的log没有leader的多，感觉不可能
                        let reply = AppendEntriesReply {
                            term: raft.state.term(),
                            success: false,
                            matchIndex:0,
                        };
                        Box::new(future::result(Ok(reply)))  
                    }
                    else {
                        let log:Vec<u8> = raft.log[prevLogIndex-1].clone();
                        let log:LogEntry = labcodec::decode(&log).unwrap();
                        if log.term == args.prevLogTerm {//如果匹配 (2)
                            //  因为是心跳包，没有entries 不进行（3）（4）
                            if args.leaderCommit > raft.commitIndex{ //（5）判断
                                let raft = Arc::clone(&self.raft);
                                let mut raft = raft.lock().unwrap();
                                raft.commitIndex = args.leaderCommit;
                                println!("{}:commitIndex change to {} by heartbeats", raft.me,raft.commitIndex);
                            }
                            let reply = AppendEntriesReply {
                                term: raft.state.term(),
                                success: true,
                                matchIndex:log.index,
                            };
                            Box::new(future::result(Ok(reply)))  
                        }
                        else {
                            let reply = AppendEntriesReply {
                                term: raft.state.term(),
                                success: false,
                                matchIndex :0,
                            };
                            Box::new(future::result(Ok(reply)))  
                        }
                    } 
                } 
            }
            else {
                // append entries
                if prevLogIndex == 0{ //(2) 第一个log,必匹配
                    let index = raft.log.len();
                    if index == 0{ 
                        //接受方没有log,直接append
                        let raft = Arc::clone(&self.raft);
                        let mut raft = raft.lock().unwrap();
                        raft.log.push(args.entries.clone());
                        let reply = AppendEntriesReply {
                            term: raft.state.term(),
                            success: true,
                            matchIndex:1,
                        };
                        Box::new(future::result(Ok(reply)))
                    }
                    else {
                        //接受方有log，进行（3）
                        let entries = args.entries.clone();
                        let entries:LogEntry = labcodec::decode(&entries).unwrap();
                        let log = raft.log[0].clone();
                        let log:LogEntry = labcodec::decode(&log).unwrap();
                        if entries.term != log.term { 
                            //（3）判断 ，由于实现的entries只有一条，如果相等不进行（4）
                            //（3）失败，进行（4）
                            let raft = Arc::clone(&self.raft);
                            let mut raft = raft.lock().unwrap();
                            raft.log.clear();
                            raft.log.push(args.entries.clone());
                        }
                        if args.leaderCommit > raft.commitIndex{ //（5）判断
                            let changeIndex = {
                                if entries.index < args.leaderCommit {
                                    entries.index
                                }
                                else {
                                    args.leaderCommit
                                }
                            };
                            let raft = Arc::clone(&self.raft);
                            let mut raft = raft.lock().unwrap();
                            raft.commitIndex = changeIndex;
                            println!("{}:commitIndex change to {} by appendlogs", raft.me,raft.commitIndex);
                        }
                        let reply = AppendEntriesReply {
                            term: raft.state.term(),
                            success: true,
                            matchIndex:entries.index,
                        };
                        Box::new(future::result(Ok(reply)))  
                    }
                }
                else {
                    let index = raft.log.len();
                    // let entries = args.entries.clone();
                    // let entries:LogEntry = labcodec::decode(&entries).unwrap();
                    // let entry = entries.ord.clone();
                    // let entry:Entry = labcodec::decode(&entry).unwrap();
                    // println!("{}:{:?} {:?}",raft.me,args,entry);

                    if index < prevLogIndex { //接受方log少于pre,按理说不可能。。
                        let reply = AppendEntriesReply {
                            term: raft.state.term(),
                            success: false,
                            matchIndex:0,
                        };
                        Box::new(future::result(Ok(reply)))
                    }
                    else {
                        let log = raft.log[prevLogIndex-1].clone();
                        let log:LogEntry = labcodec::decode(&log).unwrap();
                        if log.term != args.prevLogTerm { //(2)不匹配
                            let reply = AppendEntriesReply {
                                term: raft.state.term(),
                                success: false,
                                matchIndex:0,
                            };
                            Box::new(future::result(Ok(reply)))
                        }
                        else { //(2)匹配
                            let entries = args.entries.clone();
                            let entries:LogEntry = labcodec::decode(&entries).unwrap();
                            if entries.index > (index as u64) { 
                                //表示entries是接受方没有的新log,直接(4)
                                let raft = Arc::clone(&self.raft);
                                let mut raft = raft.lock().unwrap();
                                raft.log.push(args.entries.clone());
                            }
                            else {
                                let log = raft.log[(entries.index-1) as usize].clone();
                                let log:LogEntry = labcodec::decode(&log).unwrap();
                                if entries.term != log.term { 
                                    //(3) 冲突
                                    let raft = Arc::clone(&self.raft);
                                    let mut raft = raft.lock().unwrap();
                                    raft.log.truncate((entries.index-1) as usize);
                                    raft.log.push(args.entries.clone());
                                }
                                else {
                                    //(3) 无冲突，但是由于entries只有1个，也就意味着不用进行(4)
                                }
                            }
                            //进行（5）
                            if args.leaderCommit > raft.commitIndex{ //（5）判断
                                let changeIndex = {
                                    if entries.index < args.leaderCommit {
                                        entries.index
                                    }
                                    else {
                                        args.leaderCommit
                                    }
                                };
                                let raft = Arc::clone(&self.raft);
                                let mut raft = raft.lock().unwrap();
                                raft.commitIndex = changeIndex;
                                println!("{}:commitIndex change to {} by appendlogs", raft.me,raft.commitIndex);

                            }
                            let reply = AppendEntriesReply {
                                term: raft.state.term(),
                                success: true,
                                matchIndex: entries.index,
                            };
                            // println!("{:?}", reply);
                            Box::new(future::result(Ok(reply))) 
                        }
                    }
                }
            }
        }  
    }
}
