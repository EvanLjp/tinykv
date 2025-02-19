// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout     int
	baseElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress, len(c.peers))
	votes := make(map[uint64]bool, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = new(Progress)
		votes[peer] = false
	}
	r := &Raft{
		id:                  c.ID,
		State:               StateFollower,
		RaftLog:             newLog(c.Storage),
		heartbeatTimeout:    c.HeartbeatTick,
		baseElectionTimeout: c.ElectionTick,
		Prs:                 prs,
		votes:               votes,
	}
	rand.Seed(time.Now().UnixNano())
	r.randomElection()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	// Your Code Here (2A).
	r.heartbeatElapsed = 0
	for to := range r.Prs {
		if r.id == to {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      to,
			Term:    r.Term,
		})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.votes = nil
	log.Infof("Node[%d] become to follower, leader is Node[%d]", r.id, r.Lead)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.Vote = r.id
	r.Lead = 0
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	log.Infof("Node[%d] become to candidate: term(%d)", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.votes = nil
	r.Lead = r.id
	for _, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
	}
	log.Infof("Node[%d] become to leader: term(%d),index(%d)", r.id, r.Term, r.RaftLog.LastIndex())
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		case pb.MessageType_MsgBeat:
			r.sendHeartbeat()
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		}
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		}
	case StateCandidate:
		switch m.GetMsgType() {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		}
	}
	return nil
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) requestElection() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	index := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(index)
	r.randomElection()
	for id := range r.Prs {
		if id == r.id {
			r.votes[r.id] = true
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				Index:   index,
				LogTerm: logTerm,
			})
		}
	}
}

func (r *Raft) randomElection() {
	r.electionTimeout = r.baseElectionTimeout + rand.Intn(r.baseElectionTimeout)
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		offset := r.Prs[id].Next - 1 - r.RaftLog.stabled
		ents := r.RaftLog.entries[offset:]
		var entries []*pb.Entry
		for i := 0; i < len(ents); i++ {
			entries = append(entries, &ents[i])
		}
		if len(entries) == 0 {
			continue
		}
		logTerm, _ := r.RaftLog.Term(r.Prs[id].Next)

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: logTerm,
			Index:   r.Prs[id].Match,
			Entries: entries,
			Commit:  r.RaftLog.committed,
		})
	}
}
