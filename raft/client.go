package raft

import (
	"math/rand"
	"sync"
	"time"
)

// A client for a Raft cluster
type Client struct {
	bigLock sync.Mutex

	clientId  int64
	requestId int64

	nodes  []string
	leader string
}

// Makes a new client with the given cluster
func NewClient(nodes []string) *Client {
	// Choose a random clientId so we shouldn't collide with any other clients
	c := &Client{
		clientId:  rand.Int63(),
		requestId: 0,
		nodes:     nodes,
		leader:    "",
	}

	return c
}

// Helper to pick a random node from the cluster
func (c *Client) randomNode() string {
	index := rand.Intn(len(c.nodes))
	return c.nodes[index]
}

// Passes a request to the leader of the cluster
// Automatically follows redirects and re-randomises until something claiming to be the leader responds
func (c *Client) callLeader(name string, req interface{}) interface{} {
	for {
		// If we don't have a leader, rerandomise
		if c.leader == "" {
			c.leader = c.randomNode()
		}

		// If connecting to the leader failed, rerandomise
		conn, err := dialWithTimeout(c.leader, 5*time.Second)
		if err != nil {
			c.leader = c.randomNode()
			continue
		}

		// If the request failed, rerandomise
		var resp ClientResponse
		if err := conn.Call(name, req, &resp); err != nil {
			c.leader = c.randomNode()
			conn.Close()
			continue
		}

		// If we weren't redirected, we're done, return the data
		if !resp.Redirected {
			conn.Close()
			return resp.Data
		}

		// Set our leader to be whatever we were given as the leader in the response
		c.leader = resp.Leader
		conn.Close()
	}
}

// Add a new log entry to the Raft log.
// Blocks until entry committed
func (c *Client) AddLogEntry(data interface{}) interface{} {
	c.bigLock.Lock()
	defer c.bigLock.Unlock()

	req := AddLogEntryRequest{
		ClientId:  c.clientId,
		RequestId: c.requestId,
		Data:      data,
	}

	// Request IDs must be unique and serial, so increment after each request
	c.requestId++

	return c.callLeader("Raft.AddLogEntry", req)
}

// Perform a read-only request to the cluster
func (c *Client) Get(name string, data interface{}) interface{} {
	c.bigLock.Lock()
	defer c.bigLock.Unlock()

	req := GetRequest{
		Name: name,
		Data: data,
	}

	return c.callLeader("Raft.Get", req)
}
