package errs

import (
	cou "github.com/nj-eka/MemcLoadGo/ctxutils"
	"time"
)

type errorData struct {
	err      error
	severity Severity
	kind     Kind
	ops      cou.Operations
	frames   []Frame
	ts       int64
}

// just for checking
var _ Error = &errorData{}
var _ Error = (*errorData)(nil)

// newError set default values and return Error
func newError() Error {
	return errorData{kind: KindOther, severity: SeverityError, ts: time.Now().UnixNano()}
}

func (e errorData) TimeStamp() time.Time {
	return time.Unix(0, e.ts)
}

func (e errorData) Error() string {
	return e.err.Error()
	//fmt.Sprintf("%q", e.err.Error())
	//fmt.Sprintf("{ops: <%s> sfs: <%s>; err: <%v>", e.ops, e.frames, e.err)
}

func (e errorData) Severity() Severity {
	return e.severity
}

func (e errorData) Kind() Kind {
	return e.kind
}

func (e errorData) OperationPath() cou.Operations {
	return e.ops
}

func (e errorData) StackTrace() []Frame {
	return e.frames
}

func (e errorData) Unwrap() error {
	return e.err
}
