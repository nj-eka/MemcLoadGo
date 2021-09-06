package logging

import (
	"context"
	"fmt"
	cou "github.com/nj-eka/MemcLoadGo/context_utils"
	"github.com/sirupsen/logrus"
	"time"
)

func Msg(args ...interface{}) *logrus.Entry {
	entry := logrus.WithFields(logrus.Fields{
		"cts":  time.Now().Format(DEFAULT_TIME_FORMAT),
		"rec":  "msg",
		"type": "string",
	})
	if len(args) == 1 {
		switch arg := args[0].(type) {
		case cou.Operation, cou.Operations:
			return entry.WithField("ops", fmt.Sprintf("%s", arg))
		case context.Context:
			return entry.WithContext(arg)
		}
	}
	return entry
	//return logrus.WithFields(logrus.Fields{
	//	"ops": cou.GetContextOperations(ctx),
	//	"rec":  "msg",
	//	"type": "string",
	//})
}
