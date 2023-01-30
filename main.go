package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/chamlis/daft/raft"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type Increment struct {
	lock  sync.RWMutex
	value int
}

type IncrementCommand struct {
	Id int
}
type TripleCommand struct {
	Id int
}

type GetResponse struct {
	Value int
}

func (i *Increment) Update(command interface{}) {
	switch command.(type) {
	case IncrementCommand:
		i.value++
	case TripleCommand:
		i.value *= 3
	}
}

func (i *Increment) Get(req interface{}) interface{} {
	i.lock.RLock()
	defer i.lock.RUnlock()

	return GetResponse{
		Value: i.value,
	}
}

func sendCommands(cluster []string) {
	client := raft.NewClient(cluster)
	commandInterval := time.NewTicker(50 * time.Millisecond)

	commandsSent := 0

	for {
		<-commandInterval.C

		var resp interface{}

		if rand.Intn(2) == 0 {
			resp = client.AddLogEntry(IncrementCommand{
				Id: rand.Intn(1000),
			})
		} else {
			resp = client.AddLogEntry(TripleCommand{
				Id: rand.Intn(1000),
			})
		}

		fmt.Println("Reply", resp.(int))

		commandsSent++

		if commandsSent%10 == 0 {
			resp := client.Get("GetValue", nil).(GetResponse)
			fmt.Println("Read value", resp.Value)
		}
	}
}

func main() {
	myAddr := flag.String("me", "", "This node ID")
	cluster := flag.String("cluster", "", "Comma seperated nodes")
	stateFile := flag.String("state", "", "File to store state")
	flag.Parse()

	if *myAddr == "" || *cluster == "" || *stateFile == "" {
		fmt.Println("Bad options")
		return
	}

	gob.Register(IncrementCommand{})
	gob.Register(TripleCommand{})
	gob.Register(GetResponse{})

	rand.Seed(time.Now().UnixNano())

	s := raft.NewSqliteStorage(*stateFile)

	state := Increment{
		value: 0,
	}

	commits := make(chan raft.Commit, 16)

	clusterSplit := strings.Split(*cluster, ",")

	r := raft.NewRaft(s, commits, *myAddr, clusterSplit)
	r.RegisterGet("GetValue", state.Get)
	go r.Serve()

	commandsApplied := 0

	go sendCommands(clusterSplit)

	for {
		commit := <-commits

		state.lock.Lock()
		state.Update(commit.Data)

		commit.Response <- state.value
		close(commit.Response)

		state.lock.Unlock()
		commandsApplied++
		if commandsApplied%25 == 0 {
			fmt.Println(commandsApplied, "state is", state.value)

		}
	}

	r.Shutdown()
}
