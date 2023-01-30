package raft

import (
	"encoding/gob"
	"fmt"
	"os"
)

// A simple storage backend that just dumps the Raft state directly into a file
type FileStorage struct {
	Filename  string
	Term      int
	HaveVoted bool
	VotedFor  string
	Log       []LogEntry
}

func init() {
	gob.Register(FileStorage{})
}

// Make a new file storage with the given name
func NewFileStorage(filename string) *FileStorage {
	// If we can open the file, restore from it
	if f, err := os.Open(filename); err == nil {
		defer f.Close()

		d := gob.NewDecoder(f)
		fs := FileStorage{}

		if err = d.Decode(&fs); err != nil {
			fmt.Println(err)
			panic("Unable to load state")
		}

		fmt.Println("Loaded state in term", fs.Term)

		return &fs
	}

	// Otherwise just make a new empty FileStorage
	fs := &FileStorage{
		Filename:  filename,
		Term:      0,
		HaveVoted: false,
		VotedFor:  "",
	}

	return fs
}

// Executes a storage transaction against this backend
func (fs *FileStorage) Execute(t *Transaction) (interface{}, error) {
	value, err := t.f(nil)

	// If the transaction failed, return the error
	if err != nil {
		return nil, err
	}

	// If we failed to save the file, return the error
	if err := fs.save(); err != nil {
		return nil, err
	}

	// We succeeded, return the result
	return value, nil
}

// Get the current term from the storage
func (fs *FileStorage) GetTerm() *Transaction {
	return PureTransaction(fs.Term)
}

// Get the current chosen candidate from the storage
func (fs *FileStorage) GetVotedFor() *Transaction {
	return PureTransaction([]interface{}{fs.HaveVoted, fs.VotedFor})
}

// Get the current log from the storage
func (fs *FileStorage) GetLog() *Transaction {
	return PureTransaction(fs.Log)
}

// Helper function to serialize the storage to a file
func (fs *FileStorage) save() error {
	f, err := os.Create(fs.Filename)
	if err != nil {
		return err
	}

	defer f.Close()

	e := gob.NewEncoder(f)
	if err = e.Encode(*fs); err != nil {
		return err
	}

	return nil
}

// Set the current term
func (fs *FileStorage) SetTerm(term int) *Transaction {
	return &Transaction{
		f: func(_ interface{}) (interface{}, error) {
			fs.Term = term
			return nil, nil
		},
	}
}

// Set the chosen candidate
func (fs *FileStorage) SetVotedFor(voted bool, id string) *Transaction {
	return &Transaction{
		f: func(_ interface{}) (interface{}, error) {
			fs.HaveVoted, fs.VotedFor = voted, id
			return nil, nil
		},
	}
}

// Append an entry to the log
func (fs *FileStorage) AppendLog(entry LogEntry) *Transaction {
	return &Transaction{
		f: func(_ interface{}) (interface{}, error) {
			fs.Log = append(fs.Log, entry)
			return nil, nil
		},
	}
}

// Trim the log, deleting all entries after a certain index
func (fs *FileStorage) TrimLog(trimFrom int) *Transaction {
	return &Transaction{
		f: func(_ interface{}) (interface{}, error) {
			fs.Log = fs.Log[:trimFrom]
			return nil, nil
		},
	}
}
