package errs

type Kind uint32

// todo: make kind values as flags so that masks can be used to classify compound errors
// const KindOther Kind = 1 << (32 - 1 - iota)
const (
	KindOther        Kind = iota // Unclassified error. This value is not printed in the error message.
	KindTransient                // Transient error  todo: use prev Error values
	KindInterrupted              // Some kind of inconsistency
	KindIO                       // External I/O error such as network failure.
	KindInvalidValue             // Invalid value for this type of item.
	KindNotExist                 // Item does not exist.
	KindOpenFile                 // os.Open errors
	KindGzip                     // gzip errors
	KindProto                    // proto error
	KindDBuff                    // dbuffer error
	KindMemcache                 // memcache error
	KindInternal                 // Internal error or inconsistency.
)

func (k Kind) String() string {
	switch k {
	case KindOther:
		return "other"
	case KindTransient:
		return "transient"
	case KindInterrupted:
		return "interrupted"
	case KindIO:
		return "IO"
	case KindInvalidValue:
		return "invalid value"
	case KindNotExist:
		return "not exist"
	case KindOpenFile:
		return "file open"
	case KindGzip:
		return "gzip"
	case KindProto:
		return "proto"
	case KindDBuff:
		return "dbuffer"
	case KindMemcache:
		return "memcache"
	case KindInternal:
		return "internal"
	}
	return "unknown"
}
