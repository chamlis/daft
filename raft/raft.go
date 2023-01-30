package raft

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Represents an entry in the Raft log
type LogEntry struct {
	ClientId  int64
	RequestId int64

	Term int
	Data interface{}
}

// The commit object as handled by the Raft server
type Commit struct {
	Data     interface{}
	Response chan<- interface{}
}

// The request to append entries onto a follower's log
type AppendEntriesRequest struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// The request to try and gather votes from nodes
type RequestVoteRequest struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// The request to add a new log onto the leader
type AddLogEntryRequest struct {
	ClientId  int64
	RequestId int64
	Data      interface{}
}

// The request to get data from the leader
type GetRequest struct {
	Name string
	Data interface{}
}

// The common response type used to handle redirecting clients
type ClientResponse struct {
	Redirected bool
	Leader     string
	Data       interface{}
}

// The type of a function that can be registered as a Get
type GetRPC func(interface{}) interface{}

// Wraps Raft to only export certain functions
type RaftRPC struct {
	raft *Raft
}

func (r *RaftRPC) AppendEntries(req AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return r.raft.AppendEntries(req, resp)
}

func (r *RaftRPC) RequestVote(req RequestVoteRequest, resp *RequestVoteResponse) error {
	return r.raft.RequestVote(req, resp)
}

func (r *RaftRPC) AddLogEntry(req AddLogEntryRequest, resp *ClientResponse) error {
	return r.raft.AddLogEntry(req, resp)
}

func (r *RaftRPC) Get(req GetRequest, resp *ClientResponse) error {
	return r.raft.Get(req, resp)
}

// The different states a node can be in
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

var NoSuchRPC = errors.New("No such RPC")

type Raft struct {
	bigLock sync.Mutex

	rpcServer *rpc.Server
	shutdown  chan bool

	// Persistent
	storage     Storage
	currentTerm int
	haveVoted   bool
	votedFor    string
	log         []LogEntry

	// Volatile
	nodeState     int // FOLLOWER, CANDIDATE, or LEADER
	commitIndex   int
	lastApplied   int
	self          string
	cluster       []string
	electionTimer *time.Ticker

	lastClientRequests  map[int64]int64
	lastClientResponses map[int64]interface{}

	commitIndexWait *sync.Cond
	commits         chan<- Commit

	leader string

	// Leaders only
	nextIndex  map[string]int
	matchIndex map[string]int

	// Get request routing
	getLock     sync.RWMutex
	getRequests map[string]GetRPC
}

func NewRaft(storage Storage, commits chan<- Commit, self string, cluster []string) *Raft {
	// Attempt to restore persistent state from storage
	resRaw, err := storage.Execute(SequenceTransactions(
		storage.GetTerm(),
		storage.GetLog(),
		storage.GetVotedFor(),
	))

	// If we couldn't, don't proceed
	if err != nil {
		panic(err)
	}

	// Initialize Raft
	res := resRaw.([]interface{})

	r := &Raft{
		rpcServer:           rpc.NewServer(),
		shutdown:            make(chan bool),
		storage:             storage,
		currentTerm:         res[0].(int),
		log:                 res[1].([]LogEntry),
		nodeState:           FOLLOWER,
		commitIndex:         -1,
		self:                self,
		cluster:             cluster,
		electionTimer:       time.NewTicker(time.Hour),
		lastClientRequests:  make(map[int64]int64),
		lastClientResponses: make(map[int64]interface{}),
		commits:             commits,
		nextIndex:           make(map[string]int),
		matchIndex:          make(map[string]int),
		getRequests:         make(map[string]GetRPC),
	}

	r.commitIndexWait = sync.NewCond(&r.bigLock)

	r.haveVoted, r.votedFor = res[2].([]interface{})[0].(bool), res[2].([]interface{})[1].(string)

	// Randomise the inital election timer
	r.resetElectionTimer()

	// Setup and register the RPC wrapper
	rRPC := RaftRPC{
		raft: r,
	}

	r.rpcServer.RegisterName("Raft", &rRPC)

	return r
}

// Registers a Get callback with a given name
func (r *Raft) RegisterGet(name string, handler GetRPC) {
	r.getLock.Lock()
	defer r.getLock.Unlock()
	r.getRequests[name] = handler
}

// Registers a raw RPC object
func (r *Raft) RegisterRaw(value interface{}) {
	r.bigLock.Lock()
	defer r.bigLock.Unlock()

	r.rpcServer.Register(value)
}

// Helper function to collect connections from listener
func listen(l net.Listener, conns chan<- net.Conn) {
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		conns <- conn
	}
}

// Start Raft serving requests
func (r *Raft) Serve() {
	// Only listen on port as address
	port := r.self[strings.Index(r.self, ":"):]
	l, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	// Start background goroutines
	go r.leaderHeartbeat()
	go r.electionWatchdog()

	// Handle connections such that we can shutdown cleanly
	connections := make(chan net.Conn)
	go listen(l, connections)

	var connectionsGroup sync.WaitGroup

acceptLoop:
	for {
		select {
		case _, ok := <-r.shutdown:
			if !ok {
				break acceptLoop
			}
			break
		case conn := <-connections:
			connectionsGroup.Add(1)
			go (func() {
				r.rpcServer.ServeConn(conn)
				connectionsGroup.Done()
			})()
			break
		}
	}

	log.Println("Shutdown - waiting for connections to die...")
	connectionsGroup.Wait()
}

// Stop Raft serving requests
func (r *Raft) Shutdown() {
	close(r.shutdown)
}

// Helper function to log the current state of the node to STDOUT
func (r *Raft) logStatus() {
	var stateStr string

	switch r.nodeState {
	case LEADER:
		stateStr = "LEADER"
	case CANDIDATE:
		stateStr = "CANDIDATE"
	case FOLLOWER:
		stateStr = "FOLLOWER"
	}

	log.Printf("[%d] %s\n", r.currentTerm, stateStr)
}

// Helper function to get the last term and index from the log
func (r *Raft) getLastTermAndIndex() (int, int) {
	if len(r.log) == 0 {
		return -1, -1
	}

	ix := len(r.log) - 1
	return r.log[ix].Term, ix
}

// Rerandomises the election timer
func (r *Raft) resetElectionTimer() {
	milliseconds := rand.Intn(500) + 300
	electionDelay := time.Duration(milliseconds) * time.Millisecond
	r.electionTimer.Reset(electionDelay)
}

// Helper function to execute transaction against storage and panic on error
func (r *Raft) executeStorage(t *Transaction) interface{} {
	value, err := r.storage.Execute(t)

	if err != nil {
		panic(err)
	}

	return value
}

// Background goroutine to heartbeat regularly
func (r *Raft) leaderHeartbeat() {
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		<-ticker.C

		r.bigLock.Lock()

		// If we're not leader, don't do anything
		if r.nodeState != LEADER {
			r.bigLock.Unlock()
			continue
		}

		// Issue requests in parallel, so wait for them all to complete
		var wg sync.WaitGroup

		for _, node := range r.cluster {
			node := node // :(

			// Don't talk to ourselves
			if node == r.self {
				continue
			}

			// Lookup values for given node and make request
			lastLogIndex := r.nextIndex[node] - 1

			var lastLogTerm int

			if lastLogIndex < 0 {
				lastLogTerm = -1
			} else {
				lastLogTerm = r.log[lastLogIndex].Term
			}

			req := AppendEntriesRequest{
				Term:         r.currentTerm,
				LeaderId:     r.self,
				PrevLogIndex: lastLogIndex,
				PrevLogTerm:  lastLogTerm,
				Entries:      r.log[lastLogIndex+1:],
				LeaderCommit: r.commitIndex,
			}

			wg.Add(1)

			go (func() {
				defer wg.Done()

				c, err := dialWithTimeout(node, 250*time.Millisecond)
				if err != nil {
					return
				}
				defer c.Close()

				var resp AppendEntriesResponse

				if err := c.Call("Raft.AppendEntries", req, &resp); err != nil {
					return
				}

				// We need to relock here to modify state of follower in leader
				r.bigLock.Lock()
				defer r.bigLock.Unlock()

				// If follower had a newer term we are no longer leader so stop right here
				if !r.checkTerm(resp.Term) {
					return
				}

				if resp.Success {
					// If we succeeded update nextIndex and matchIndex

					newNextIndex := lastLogIndex + len(req.Entries) + 1

					if newNextIndex > r.nextIndex[node] {
						r.nextIndex[node] = newNextIndex
					}

					r.matchIndex[node] = r.nextIndex[node] - 1
				} else {
					// Otherwise decrement nextIndex for next heartbeat
					r.nextIndex[node] -= 1
				}
			})()
		}

		r.bigLock.Unlock()

		// Wait for all requests to finish
		wg.Wait()

		r.bigLock.Lock()

		oldCommitIndex := r.commitIndex
		majority := (len(r.cluster) / 2) + 1

		// Calculate the highest index that has been synced to a majority of followers
		for ix := r.commitIndex + 1; ix < len(r.log); ix++ {
			if r.log[ix].Term != r.currentTerm {
				continue
			}

			nodesExceed := 1

			for _, node := range r.cluster {
				if r.matchIndex[node] >= ix {
					nodesExceed++
				}
			}

			if nodesExceed >= majority {
				r.commitIndex = ix
			}
		}

		// Commit log entries that have been synced to a majority
		for ix := oldCommitIndex + 1; ix <= r.commitIndex; ix++ {
			r.commitLogEntry(r.log[ix])
		}

		// If we've committed more, broadcast on condvar to wake up any AppendEntry RPCs waiting to return to client
		if r.commitIndex > oldCommitIndex {
			//log.Println("Committed up to", r.commitIndex)
			r.commitIndexWait.Broadcast()
		}

		r.bigLock.Unlock()
	}
}

func (r *Raft) electionWatchdog() {
	for {
		// Wait for timer to expire
		<-r.electionTimer.C

		r.bigLock.Lock()
		// If we're the leader, don't do anything
		if r.nodeState == LEADER {
			r.bigLock.Unlock()
			continue
		}
		r.bigLock.Unlock()

		log.Println("Election watchdog expired")

		// Become a candidate
		r.becomeCandidate()
	}
}

func (r *Raft) becomeCandidate() {
	log.Println("Becoming candidate")

	r.bigLock.Lock()

	// Reset state and increment term
	r.nodeState = CANDIDATE

	r.currentTerm++

	// Vote for self
	r.haveVoted = true
	r.votedFor = r.self

	// Update storage with new state
	r.executeStorage(Sequence_Transactions(
		r.storage.SetTerm(r.currentTerm),
		r.storage.SetVotedFor(true, r.self),
	))

	r.resetElectionTimer()

	// Calculate needed votes and start ballot
	neededVotes := uint64((len(r.cluster) / 2) + 1)
	var votes uint64 = 1 // already voted for by self

	myLastTerm, myLastIndex := r.getLastTermAndIndex()

	// Prepare request
	req := RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateId:  r.self,
		LastLogIndex: myLastIndex,
		LastLogTerm:  myLastTerm,
	}

	// Store current term to check if it changes during election
	oldTerm := r.currentTerm

	r.bigLock.Unlock()

	var wg sync.WaitGroup

	for _, node := range r.cluster {
		node := node

		// Don't ask ourselves for a vote
		if node == r.self {
			continue
		}

		wg.Add(1)

		go (func() {
			defer wg.Done()

			c, err := dialWithTimeout(node, 100*time.Millisecond)
			if err != nil {
				return
			}

			defer c.Close()

			var resp RequestVoteResponse

			if err := c.Call("Raft.RequestVote", req, &resp); err != nil {
				log.Println("RequestVote error", err)
				return
			}

			// If we have an old term, stop right here
			if oldTerm < resp.Term {
				return
			}

			// Atomically increment the vote count if we got the vote
			if resp.VoteGranted {
				atomic.AddUint64(&votes, 1)
			}
		})()
	}

	// Wait for all votes to be requests
	wg.Wait()

	log.Println("Got", votes, ", needed", neededVotes)

	// Relock to update state
	r.bigLock.Lock()

	// If term changed, don't become leader
	if oldTerm != r.currentTerm {
		log.Println("Term changed while getting votes, ignoring election", oldTerm, "->", r.currentTerm)
		r.bigLock.Unlock()
		return
	}

	if votes < neededVotes {
		r.bigLock.Unlock()
		return
	}

	// If we got enough votes - we are now the leader of this term!
	go r.becomeLeader()
}

func (r *Raft) becomeLeader() {
	defer r.bigLock.Unlock()
	log.Println("Becoming leader in term", r.currentTerm)

	r.nodeState = LEADER

	// Reset leader-only state
	for _, node := range r.cluster {
		r.nextIndex[node] = len(r.log)
		r.matchIndex[node] = -1
	}

	// Issue noop LogEntry to maintain invariants
	noop := LogEntry{
		ClientId:  rand.Int63(),
		RequestId: 0,
		Term:      r.currentTerm,
		Data:      nil,
	}

	r.addLogEntryAndWaitForCommit(noop)

	r.logStatus()
}

// Helper function to check incoming term on any request and become follower if we're outdated
func (r *Raft) checkTerm(prospective int) bool {
	switch {
	case prospective < r.currentTerm:
		return false
	case prospective > r.currentTerm:
		log.Println("Term out of date, becoming follower", r.currentTerm, "<", prospective)
		r.currentTerm = prospective
		r.nodeState = FOLLOWER
		r.haveVoted = false
		r.logStatus()
		return true
	default:
		return true
	}
}

// Called from other Raft nodes to append entries to our log
func (r *Raft) AppendEntries(req AppendEntriesRequest, resp *AppendEntriesResponse) error {
	r.bigLock.Lock()
	defer r.bigLock.Unlock()

	// Always update sender with our current term
	resp.Term = r.currentTerm

	// If we're out of date don't handle this
	if !r.checkTerm(req.Term) {
		resp.Success = false
		return nil
	}

	// If we were asking for votes but a new leader came along, we follow them now
	if r.nodeState == CANDIDATE {
		log.Println("New leader, becoming follower")
		r.nodeState = FOLLOWER
		r.logStatus()
	}

	// Update our notion of leader
	r.leader = req.LeaderId

	// We heard from leader so reset timeout
	r.resetElectionTimer()

	// If the leader is trying to sync from a point in the log we don't have, fail the request so they'll try and sync from earlier
	if req.PrevLogIndex != -1 {
		if req.PrevLogIndex >= len(r.log) || r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			resp.Success = false
			return nil
		}
	}

	// Update log with new entries, building up a storage transaction as we go
	lastEntryIndex := len(r.log) + len(req.Entries) - 1

	var transactions []*Transaction

	for ix := 0; ix < len(req.Entries); ix++ {
		logIndex := req.PrevLogIndex + ix + 1

		if logIndex < len(r.log) {
			// If it's within our log, and the terms don't match, replace it
			if req.Entries[ix].Term != r.log[logIndex].Term {
				r.log = r.log[:logIndex]
				r.log = append(r.log, req.Entries[ix])

				transactions = append(transactions,
					r.storage.TrimLog(logIndex),
					r.storage.AppendLog(req.Entries[ix]),
				)
			}
		} else {
			// Otherwise just append it
			r.log = append(r.log, req.Entries[ix])
			transactions = append(transactions, r.storage.AppendLog(req.Entries[ix]))
		}
	}

	// Sequence and execute all the above transactions
	r.executeStorage(Sequence_Transactions(transactions...))

	// If the leader has committed more entries than us, we can commit them too!
	if req.LeaderCommit > r.commitIndex {
		oldCommitIndex := r.commitIndex

		if req.LeaderCommit < lastEntryIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastEntryIndex
		}

		for ix := oldCommitIndex + 1; ix <= r.commitIndex; ix++ {
			r.commitLogEntry(r.log[ix])
		}

		//log.Println("Committing up to", r.commitIndex)
	}

	resp.Success = true

	return nil
}

// Called by other Raft nodes to request our vote
func (r *Raft) RequestVote(req RequestVoteRequest, resp *RequestVoteResponse) error {
	r.bigLock.Lock()
	defer r.bigLock.Unlock()

	log.Println("Vote request from", req.CandidateId)

	// Always tell sender our current term
	resp.Term = r.currentTerm

	// Don't give votes to prior terms
	if !r.checkTerm(req.Term) {
		log.Println("  Denying vote because term out of date", req.Term, "<", r.currentTerm)
		resp.VoteGranted = false
		return nil
	}

	// If we've already voted, don't vote again
	if r.haveVoted && r.votedFor != req.CandidateId {
		log.Println("  Denying vote because already voted for", r.votedFor)
		resp.VoteGranted = false
		return nil
	}

	// Check the candidate's log is at least as up to date as our own
	myLastTerm, myLastIndex := r.getLastTermAndIndex()

	if (req.LastLogTerm == myLastTerm && req.LastLogIndex < myLastIndex) || (req.LastLogTerm < myLastTerm) {
		log.Println("  Denying vote because log out of date")
		log.Println(req.LastLogTerm, req.LastLogIndex, "<", myLastTerm, myLastIndex)
		resp.VoteGranted = false
		return nil
	}

	log.Println("  Placing vote")

	// Give them the vote
	r.haveVoted = true
	r.votedFor = req.CandidateId
	r.resetElectionTimer()

	resp.VoteGranted = true

	r.logStatus()

	return nil
}

// Helper function to commit a log entry
func (r *Raft) commitLogEntry(entry LogEntry) {
	// Check for client serialization so we don't reapply commits
	if oldReq, ok := r.lastClientRequests[entry.ClientId]; ok {
		if oldReq > entry.RequestId {
			log.Fatalln("Client request IDs out of order, exiting to preserve safety")
			return
		} else if oldReq == entry.RequestId {
			return
		}
	}

	// Update last request for client
	r.lastClientRequests[entry.ClientId] = entry.RequestId

	// Make commit
	responseChannel := make(chan interface{})

	commit := Commit{
		Data:     entry.Data,
		Response: responseChannel,
	}

	// Hand commit off to user code and wait for response
	r.commits <- commit

	response, ok := <-responseChannel

	if !ok {
		response = nil
	}

	// Cache response
	r.lastClientResponses[entry.ClientId] = response
}

// Helper function to add a log entry and wait for it to be committed
func (r *Raft) addLogEntryAndWaitForCommit(entry LogEntry) interface{} {
	// Add it to the log
	r.log = append(r.log, entry)
	r.executeStorage(r.storage.AppendLog(entry))

	desiredCommitIndex := len(r.log) - 1

	// Wait on the condition variable until the commit index exceeds what we just added
	for r.commitIndex < desiredCommitIndex {
		//log.Println("Waiting...", r.commitIndex, "<", desiredCommitIndex)
		r.commitIndexWait.Wait()
	}

	// If we trip this the client issued another request at the same time which breaks invariants
	if r.lastClientRequests[entry.ClientId] != entry.RequestId {
		log.Fatalln("Client issuing requests in parallel? Exiting to preserve safety")
		return nil
	}

	return r.lastClientResponses[entry.ClientId]
}

// Called by clients to add a new log entry
func (r *Raft) AddLogEntry(req AddLogEntryRequest, resp *ClientResponse) error {
	r.bigLock.Lock()
	defer r.bigLock.Unlock()

	// If we're not the leader, redirect the client to who we think the leader is
	if r.nodeState != LEADER {
		resp.Redirected = true
		resp.Leader = r.leader
		return nil
	}

	resp.Redirected = false

	// Make new log entry
	newEntry := LogEntry{
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
		Term:      r.currentTerm,
		Data:      req.Data,
	}

	// Add the new entry to the log and don't return to client until it's committed
	resp.Data = r.addLogEntryAndWaitForCommit(newEntry)

	return nil
}

// Called by clients for read-only requests
func (r *Raft) Get(req GetRequest, resp *ClientResponse) error {
	r.bigLock.Lock()
	defer r.bigLock.Unlock()

	// If we're not the leader, redirect the client to who we think the leader is
	if r.nodeState != LEADER {
		resp.Redirected = true
		resp.Leader = r.leader
		return nil
	}

	// Send an empty heartbeat to the whole cluster, to verify a majority still think we're the leader

	var wg sync.WaitGroup

	neededAcks := uint64((len(r.cluster) / 2) + 1)
	var acks uint64 = 1 // already acked for by self

	for _, node := range r.cluster {
		node := node

		// Don't send to ourselves
		if node == r.self {
			continue
		}

		lastLogIndex := r.nextIndex[node] - 1

		var lastLogTerm int

		if lastLogIndex < 0 {
			lastLogTerm = -1
		} else {
			lastLogTerm = r.log[lastLogIndex].Term
		}

		heartbeat := AppendEntriesRequest{
			Term:         r.currentTerm,
			LeaderId:     r.self,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: r.commitIndex,
		}

		wg.Add(1)

		go (func() {
			defer wg.Done()

			c, err := dialWithTimeout(node, 100*time.Millisecond)
			if err != nil {
				return
			}
			defer c.Close()

			var resp AppendEntriesResponse

			if err := c.Call("Raft.AppendEntries", heartbeat, &resp); err != nil {
				return
			}

			if resp.Success {
				atomic.AddUint64(&acks, 1)
			}
		})()
	}

	// Wait for requests to finish
	wg.Wait()

	// If we didn't get enough ACKs, we're not the leader :(
	// Send the client an empty leader so they can rerandomise
	if acks < neededAcks {
		resp.Redirected = true
		resp.Leader = ""
		return nil
	}

	// Otherwise we're still a majority leader!

	// Get the correct handler for the request
	r.getLock.RLock()
	handler, ok := r.getRequests[req.Name]
	r.getLock.RUnlock()

	if !ok {
		return NoSuchRPC
	}

	// Return the result of that handler
	resp.Redirected = false
	resp.Data = handler(req.Data)
	return nil
}
