package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ServerState represents the state of a raft server.
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// LogEntry represents a single log entry
type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// Server represents a raft server.
type Server struct {
	id            int
	state         ServerState
	currentTerm   int
	votedFor      int // renamed for clarity
	currentLeader int

	mu sync.Mutex // Mutex to protect shared access to this server's state.

	// All servers
	commitIndex int
	lastApplied int

	// Log entries; index starts from 1
	log []LogEntry // log[0] is unused; first entry at log[1]

	// Leader state (reinitialized after election)
	nextIndex  []int // for each server, index of next log entry to send (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated (initialized to 0)

	// State machine (simple key-value store for demonstration)
	stateMachine map[string]string

	// Election timeout
	lastHeartbeat time.Time
	electionTimeout time.Duration // per-server randomized timeout
}

// NewServer creates a new server.
func NewServer(id int, numPeers int) *Server {
	s := &Server{
		id:            id,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		currentLeader: -1,
		commitIndex:   0,
		lastApplied:   0,
		log:           []LogEntry{{}}, // log[0] unused
		nextIndex:     make([]int, numPeers),
		matchIndex:    make([]int, numPeers),
		stateMachine:  make(map[string]string),
		lastHeartbeat: time.Now(),
	}
	s.resetElectionTimeout()
	return s
}

// resetElectionTimeout sets a new randomized election timeout (150-300ms as per Raft paper suggestion)
func (s *Server) resetElectionTimeout() {
	s.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// GetState returns the current state.
func (s *Server) GetState() ServerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// GetTerm returns the current term.
func (s *Server) GetTerm() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

// RequestVoteArgs for RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply from RequestVote RPC.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote is the RPC handler for RequestVote.
func (s *Server) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Term = s.currentTerm
	if args.Term < s.currentTerm {
		reply.VoteGranted = false
		return nil
	}
	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.votedFor = -1
		s.state = Follower
	}
	lastLogIndex, lastLogTerm := s.lastLogInfo()
	if (s.votedFor == -1 || s.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		s.votedFor = args.CandidateID
		reply.VoteGranted = true
		s.lastHeartbeat = time.Now() // Reset timeout on granting vote
	} else {
		reply.VoteGranted = false
	}
	return nil
}

// AppendEntriesArgs for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply from AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
	// For optimization: if failure, can include conflict info, but omitted for simplicity
}

// AppendEntries is the RPC handler for AppendEntries
func (s *Server) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Term = s.currentTerm

	if args.Term < s.currentTerm {
		reply.Success = false
		return nil
	}

	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.votedFor = -1
		s.state = Follower
	}

	s.currentLeader = args.LeaderID
	s.lastHeartbeat = time.Now()
	s.resetElectionTimeout() // Reset on successful heartbeat

	// Check if log contains entry at PrevLogIndex with PrevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(s.log)-1 {
			reply.Success = false
			return nil
		}
		if s.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return nil
		}
	}

	// Find insertion point for new entries
	i := args.PrevLogIndex + 1
	j := 0
	for i < len(s.log) && j < len(args.Entries) {
		if s.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}

	// Truncate conflicting entries
	if j < len(args.Entries) || i < len(s.log) {
		s.log = s.log[:i]
	}

	// Append new entries
	s.log = append(s.log, args.Entries[j:]...)

	// Update commitIndex
	if args.LeaderCommit > s.commitIndex {
		s.commitIndex = min(args.LeaderCommit, len(s.log)-1)
	}

	// Apply committed entries
	s.applyCommittedEntries()

	reply.Success = true
	return nil
}

// lastLogInfo returns the last log index and term
func (s *Server) lastLogInfo() (int, int) {
	if len(s.log) <= 1 {
		return 0, 0
	}
	last := s.log[len(s.log)-1]
	return last.Index, last.Term
}

// BecomeCandidate transitions server to candidate state and starts election.
func (s *Server) BecomeCandidate() {
	s.mu.Lock()
	s.state = Candidate
	s.currentTerm++
	s.votedFor = s.id
	s.resetElectionTimeout()
	s.mu.Unlock()
}

// BecomeFollower transitions server to follower state.
func (s *Server) BecomeFollower(term int, leaderID int) {
	s.mu.Lock()
	if term >= s.currentTerm {
		s.currentTerm = term
		s.state = Follower
		s.votedFor = -1
		s.currentLeader = leaderID
	}
	s.mu.Unlock()
}

// BecomeLeader transitions server to leader state.
func (s *Server) BecomeLeader(numPeers int) {
	s.mu.Lock()
	s.state = Leader
	s.currentLeader = s.id
	lastIndex, _ := s.lastLogInfo()
	for i := 0; i < numPeers; i++ {
		s.nextIndex[i] = lastIndex + 1
		s.matchIndex[i] = 0
	}
	s.mu.Unlock()
}

// Ticker to simulate timeouts and heartbeats.
func (s *Server) Ticker(peers []*Server, me int) {
	for {
		s.mu.Lock()
		currentState := s.state
		timeSinceHeartbeat := time.Since(s.lastHeartbeat)
		timeout := s.electionTimeout
		s.mu.Unlock()

		switch currentState {
		case Follower, Candidate: // Candidates also timeout to start new election
			if timeSinceHeartbeat > timeout {
				fmt.Printf("🔄 Server %d becoming candidate (timeout: %v)\n", s.id, timeSinceHeartbeat)
				s.BecomeCandidate()
				s.StartElection(peers, me)
			} else {
				time.Sleep(timeout - timeSinceHeartbeat)
			}
		case Leader:
			s.SendAppendEntries(peers, me, true) // Heartbeats
			time.Sleep(50 * time.Millisecond)   // Heartbeat interval (faster than election timeout)
		}
	}
}

// StartElection starts a new election.
func (s *Server) StartElection(peers []*Server, me int) {
	s.mu.Lock()
	lastLogIndex, lastLogTerm := s.lastLogInfo()
	currentTerm := s.currentTerm
	s.mu.Unlock()

	fmt.Printf("🗳️ Server %d starting election (Term: %d)\n", s.id, currentTerm)

	votes := 1 // Self-vote
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for i, peer := range peers {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(peer *Server, peerID int) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if err := peer.RequestVote(&args, &reply); err != nil {
				return // Simulate network error, ignore for now
			}
			voteMu.Lock()
			if reply.VoteGranted && reply.Term == currentTerm {
				votes++
			} else if reply.Term > currentTerm {
				s.BecomeFollower(reply.Term, -1)
			}
			voteMu.Unlock()
		}(peer, i)
	}

	wg.Wait()

	if s.GetState() != Candidate {
		return // State changed, e.g., became follower
	}

	voteMu.Lock()
	if votes > len(peers)/2 {
		fmt.Printf("🎉 Server %d WON election with %d/%d votes!\n", s.id, votes, len(peers))
		s.BecomeLeader(len(peers))
		s.SendAppendEntries(peers, me, true) // Send initial heartbeat
	} else {
		fmt.Printf("❌ Server %d LOST election with %d/%d votes\n", s.id, votes, len(peers))
		s.mu.Lock()
		s.state = Follower
		s.mu.Unlock()
	}
	voteMu.Unlock()
}

// SendAppendEntries sends append entries or heartbeats to all peers
func (s *Server) SendAppendEntries(peers []*Server, me int, heartbeat bool) {
	s.mu.Lock()
	term := s.currentTerm
	leaderCommit := s.commitIndex
	s.mu.Unlock()

	var wg sync.WaitGroup

	for i, peer := range peers {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(peer *Server, peerIndex int) {
			defer wg.Done()

			s.mu.Lock()
			next := s.nextIndex[peerIndex]
			prevIndex := next - 1
			prevTerm := 0
			if prevIndex > 0 {
				prevTerm = s.log[prevIndex].Term
			}
			entries := []LogEntry{}
			if !heartbeat && next < len(s.log) {
				entries = append(entries, s.log[next:]...)
			}
			s.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     s.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := AppendEntriesReply{}

			if err := peer.AppendEntries(&args, &reply); err != nil {
				return // Network error
			}

			s.mu.Lock()
			if reply.Term > s.currentTerm {
				s.BecomeFollower(reply.Term, -1)
				s.mu.Unlock()
				return
			}
			if reply.Success {
				if len(entries) > 0 {
					s.matchIndex[peerIndex] = entries[len(entries)-1].Index
					s.nextIndex[peerIndex] = s.matchIndex[peerIndex] + 1
				} else {
					// Heartbeat success, no change
				}
				s.updateCommitIndex()
			} else {
				// Back off nextIndex on failure
				if s.nextIndex[peerIndex] > 1 {
					s.nextIndex[peerIndex]--
				}
			}
			s.mu.Unlock()
		}(peer, i)
	}

	wg.Wait()
}

// updateCommitIndex updates the commit index based on matchIndex
func (s *Server) updateCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != Leader {
		return
	}

	// Collect all match indices including self (self's match is len(log)-1)
	match := append([]int{len(s.log) - 1}, s.matchIndex...)
	// Sort descending
	for i := 0; i < len(match)-1; i++ {
		for j := i + 1; j < len(match); j++ {
			if match[i] < match[j] {
				match[i], match[j] = match[j], match[i]
			}
		}
	}

	// The median is the commit point for majority
	majorityIndex := len(match)/2
	n := match[majorityIndex]

	if n > s.commitIndex && (n == 0 || s.log[n].Term == s.currentTerm) {
		s.commitIndex = n
		s.applyCommittedEntries()
	}
}

// applyCommittedEntries applies committed entries to the state machine
func (s *Server) applyCommittedEntries() {
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.applyToStateMachine(entry.Command)
	}
}

// applyToStateMachine applies a command to the state machine
// Assuming commands are maps like map[string]string{"op": "set", "key": "k", "value": "v"}
// or {"op": "delete", "key": "k"}
func (s *Server) applyToStateMachine(command interface{}) {
	cmd, ok := command.(map[string]string)
	if !ok {
		fmt.Printf("Invalid command: %v\n", command)
		return
	}
	op, ok := cmd["op"]
	if !ok {
		return
	}
	switch op {
	case "set":
		key := cmd["key"]
		value := cmd["value"]
		s.stateMachine[key] = value
	case "delete":
		key := cmd["key"]
		delete(s.stateMachine, key)
	case "get": // Gets are read-only, but for log, perhaps not applied this way
		// Ignore or handle
	default:
		fmt.Printf("Unknown op: %s\n", op)
	}
}

// SubmitCommand submits a command to the leader
func (s *Server) SubmitCommand(command interface{}) (bool, string) {
	s.mu.Lock()
	if s.state != Leader {
		s.mu.Unlock()
		return false, "Not leader"
	}
	lastIndex, _ := s.lastLogInfo()
	entry := LogEntry{
		Term:    s.currentTerm,
		Command: command,
		Index:   lastIndex + 1,
	}
	s.log = append(s.log, entry)
	s.matchIndex[s.id] = entry.Index // Update self match
	s.mu.Unlock()

	// Replicate to followers
	s.SendAppendEntries([]*Server{}, 0, false) // Wait, need peers; but in context, call from outside

	return true, "Command submitted"
}

// Note: In full code, SubmitCommand should take peers to send AppendEntries

// GetStateMachine returns the current state machine
func (s *Server) GetStateMachine() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]string)
	for k, v := range s.stateMachine {
		result[k] = v
	}
	return result
}

// GetLog returns the current log (from index 1)
func (s *Server) GetLog() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[1:]
}

// PrintServerState prints the current state of a server
func (s *Server) PrintServerState() {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateStr := ""
	switch s.state {
	case Follower:
		stateStr = "FOLLOWER"
	case Candidate:
		stateStr = "CANDIDATE"
	case Leader:
		stateStr = "LEADER"
	}

	lastIndex, _ := s.lastLogInfo()

	fmt.Printf("Server %d: %s (Term: %d, VotedFor: %d, CommitIndex: %d, LastApplied: %d, LogLength: %d)\n",
		s.id, stateStr, s.currentTerm, s.votedFor, s.commitIndex, s.lastApplied, lastIndex)
}

// min helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	rand.Seed(time.Now().UnixNano())
	numServers := 3
	servers := make([]*Server, numServers)
	for i := 0; i < numServers; i++ {
		servers[i] = NewServer(i, numServers)
	}

	// Start tickers
	for i, s := range servers {
		go s.Ticker(servers, i)
	}

	fmt.Println("🚀 Raft servers started. Leader election in progress...")

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Find leader
	var leader *Server
	for _, s := range servers {
		if s.GetState() == Leader {
			leader = s
			break
		}
	}

	if leader == nil {
		fmt.Println("❌ No leader elected")
		return
	}

	fmt.Printf("✅ Leader elected: Server %d\n", leader.id)

	// Submit commands (using map for KV ops)
	commands := []interface{}{
		map[string]string{"op": "set", "key": "key1", "value": "value1"},
		map[string]string{"op": "set", "key": "key2", "value": "value2"},
		map[string]string{"op": "delete", "key": "key1"},
	}

	for i, cmd := range commands {
		success, msg := leader.SubmitCommand(cmd)
		if success {
			fmt.Printf("Command %d submitted: %v -> %s\n", i+1, cmd, msg)
		} else {
			fmt.Printf("Command %d failed: %s\n", i+1, msg)
		}
		// Give time for replication
		leader.SendAppendEntries(servers, leader.id, false)
		time.Sleep(200 * time.Millisecond)
	}

	// Print final states
	fmt.Println("\n📊 Final server states:")
	for _, s := range servers {
		s.PrintServerState()
	}

	// Print leader's state machine
	fmt.Printf("\n🏪 Leader's state machine: %v\n", leader.GetStateMachine())

	// Continue simulation if needed
	time.Sleep(3 * time.Second)
	fmt.Println("🏁 Simulation ended.")
}
