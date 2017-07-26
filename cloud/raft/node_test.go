package raft_test

import (
	. "devicedb/cloud/raft"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Node", func() {
	It("should work", func() {
		raftNode := NewRaftNode(0, true)
		Expect(raftNode).Should(Not(BeNil()))
	})
})
