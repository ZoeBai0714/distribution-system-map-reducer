package raft

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type Log struct {
	Entries []Entry
	Index0  int
}