package errs

import (
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// All error in streamingservice package should be marked by streamingservice/errs package.
var (
	ErrClosed                   = merr.WrapErrServiceInternalMsg("closed")
	ErrCanceledOrDeadlineExceed = merr.WrapErrServiceInternalMsg("canceled or deadline exceed")
	ErrUnrecoverable            = merr.WrapErrServiceInternalMsg("unrecoverable")
	ErrFenced                   = merr.WrapErrServiceInternalMsg("fenced")
	ErrIgnoredOperation         = merr.WrapErrServiceInternalMsg("ignored operation")
)
