package raft

import (
	"errors"
	"net"
	"net/rpc"
	"time"
)

var ETIMEOUT = errors.New("Request timed out")

type timeoutClient struct {
	timeout time.Duration
	rpc     *rpc.Client
}

func (c *timeoutClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	timeout := time.NewTicker(c.timeout)
	call := c.rpc.Go(serviceMethod, args, reply, nil)

	select {
	case <-call.Done:
		timeout.Stop()
		return call.Error

	case <-timeout.C:
		return ETIMEOUT
	}
}

func (c *timeoutClient) Close() error {
	return c.rpc.Close()
}

func dialWithTimeout(addr string, timeout time.Duration) (*timeoutClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	client := &timeoutClient{
		timeout: timeout,
		rpc:     rpc.NewClient(conn),
	}
	return client, nil
}
