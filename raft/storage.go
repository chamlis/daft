package raft

// Monadic transactions
type Transaction struct {
	f func(interface{}) (interface{}, error)
}

// Lifts a value into the transaction
func PureTransaction(value interface{}) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			return value, nil
		},
	}
}

// Binds one transaction to the result of another
func (t *Transaction) Bind(f func(interface{}) *Transaction) *Transaction {
	return &Transaction{
		f: func(ctx interface{}) (interface{}, error) {
			value, err := t.f(ctx)

			// Propagate errors
			if err != nil {
				return nil, err
			}

			return f(value).f(ctx)
		},
	}
}

// Sequence a series of transactions and return their results
func SequenceTransactions(transactions ...*Transaction) *Transaction {
	result := PureTransaction([]interface{}{})

	for _, t := range transactions {
		t := t
		result = result.Bind(func(value interface{}) *Transaction {
			return t.Bind(func(value2 interface{}) *Transaction {
				return PureTransaction(append(value.([]interface{}), value2))
			})
		})
	}

	return result
}

// Sequence a series of transactions and ignore their results
func Sequence_Transactions(transactions ...*Transaction) *Transaction {
	result := PureTransaction(nil)

	for _, t := range transactions {
		t := t
		result = result.Bind(func(value interface{}) *Transaction {
			return t.Bind(func(value2 interface{}) *Transaction {
				return PureTransaction(nil)
			})
		})
	}

	return result
}

// Represents a storage backend
type Storage interface {
	Execute(*Transaction) (interface{}, error)

	GetTerm() *Transaction
	GetVotedFor() *Transaction
	GetLog() *Transaction

	SetTerm(int) *Transaction
	SetVotedFor(bool, string) *Transaction
	AppendLog(LogEntry) *Transaction
	TrimLog(int) *Transaction
}

// Represents storage that doesn't save anything
type NullStorage struct{}

// Executes a transaction against the storage
func (s *NullStorage) Execute(t *Transaction) (interface{}, error) {
	return t.f(nil)
}

// Gets the current term
func (s *NullStorage) GetTerm() *Transaction {
	return PureTransaction(0)
}

// Gets the chosen candidate
func (s *NullStorage) GetVotedFor() *Transaction {
	return PureTransaction([]interface{}{false, ""})
}

// Gets the current log
func (s *NullStorage) GetLog() *Transaction {
	return PureTransaction([]LogEntry{})
}

// Sets the current term
func (s *NullStorage) SetTerm(term int) *Transaction {
	return PureTransaction(nil)
}

// Sets the chosen candidate
func (s *NullStorage) SetVotedFor(voted bool, id string) *Transaction {
	return PureTransaction(nil)
}

// Appends an entry to the log
func (s *NullStorage) AppendLog(entry LogEntry) *Transaction {
	return PureTransaction(nil)
}

// Trims the log from a given index
func (s *NullStorage) TrimLog(trimFrom int) *Transaction {
	return PureTransaction(nil)
}
