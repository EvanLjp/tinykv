package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleMsgAppend(m pb.Message) {
	if m.GetMsgType() != pb.MessageType_MsgAppend {
		panic("handleMsgAppend must hand MessageType_MsgAppend msg")
	}
	reject := func(msg *pb.Message) {
		msg.Reject = true
		r.msgs = append(r.msgs, *msg)
	}
	response := func() {
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

		// todo
	}

	switch r.State {
	case StateLeader, StateCandidate:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		response()
	case StateFollower:
		response()

	}

}
