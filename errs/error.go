package errs

import (
	"context"
	"errors"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"time"
)

type Error interface {
	error
	Severity() Severity
	TimeStamp() time.Time
	Kind() Kind
	OperationPath() cu.Operations
	StackTrace() []Frame
	Unwrap() error
}

func E(args ...interface{}) Error {
	switch len(args) {
	case 0:
		panic("call to errors.E with no arguments")
	case 1:
		if e, ok := args[0].(Error); ok {
			return e
		}
	}
	e := newError().(errorData)
	// the last on the list [args] wins
	for _, arg := range args {
		switch a := arg.(type) {
		case Severity:
			e.severity = a
		case Kind:
			e.kind = a
		case cu.Operation:
			e.ops = cu.Operations{[]cu.Operation{a}}
		case cu.Operations:
			e.ops = a
		case context.Context:
			e.ops = cu.GetContextOperations(a)
		case Error: // todo: impl transient error in this case
			e.err = a
		case error:
			e.err = a
		case string:
			e.err = errors.New(a)
		default:
			// todo: 4 ways...
			//1. continue - just bypass unkown input arg type - it's default for now
			//2. panic - to panic when constructing an error might be very funny
			//3. return predefined error - and lose all original data
			//4. add to others (params & msg: as input params for fmt.Sprintf) of []interfaces{} type to build a message when needed
		}
	}
	e.frames = Trace(2)
	return e
}
