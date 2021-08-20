package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.GetMsgType() != pb.MessageType_MsgAppend {
		panic("handleAppendEntries must hand MessageType_MsgAppend msg")
	}
	reject := func(msg *pb.Message) {
		msg.Reject = true
		r.msgs = append(r.msgs, *msg)
	}
	if (r.State == StateLeader || r.State == StateCandidate) && m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	// check term
	if r.Term > m.Term {
		reject(&msg)
		return
	}
	// check log term and index
	lt, err := r.RaftLog.storage.Term(m.Index)
	if err != nil {
		log.Errorf("cannot read the term of %d index: %v", m.Index, err)
		reject(&msg)
		return
	}
	if lt != m.LogTerm {
		log.Debugf("cannot find %d index by %d term", m.Index, m.LogTerm)
		reject(&msg)
		return
	}
	r.Term = m.Term

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	isMajor := func(n int) bool {
		return n > len(r.Prs)/2
	}
	getVoteNum := func() (count int) {
		for _, vote := range r.votes {
			if vote {
				count++
			}
		}
		return
	}
	switch r.State {
	case StateCandidate:
		r.votes[m.From] = !m.Reject
		if !m.Reject {
			if isMajor(getVoteNum()) {
				r.becomeLeader()
			}
		}
	default:
		log.Debug("the vote response ignored when not candidate")
	}
}

func (r *Raft) handleVote(m pb.Message) {
	voteMsg := func(vote bool) pb.Message {
		return pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  !vote,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
		}
	}
	checkNewLog := func() bool {
		li := r.RaftLog.LastIndex()
		lt, err := r.RaftLog.Term(li)
		if err != nil {
			log.Errorf("error in getting the term of %d index, err: %v", li, err)
			return false
		}
		if lt < m.Term {
			return true
		}
		if lt == m.Term && li <= m.Index {
			return true
		}
		return false
	}
	var vote bool
	switch {
	case m.Term < r.Term:
		vote = false
	case m.Term > r.Term:
		r.becomeFollower(m.Term, 0)
		vote = checkNewLog()
	case r.Vote != 0 && r.Vote != m.From:
		vote = false
	default:
		vote = checkNewLog()
	}
	r.msgs = append(r.msgs, voteMsg(vote))
}
