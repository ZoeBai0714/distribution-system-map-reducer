package raft

import (
	"time"
	"math/rand"
)

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}