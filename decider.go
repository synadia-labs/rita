package rita

import "time"

type Command struct {
	ID   string
	Time time.Time
	Type string
	Data any
	Meta map[string]string
}

type Decider interface {
	Decide(command *Command) ([]*Event, error)
}
