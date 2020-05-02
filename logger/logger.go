package logger

import (
	"context"
	"fmt"
	"path"
	"runtime"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
	"github.com/zput/zxcTool/ztLog/zt_formatter"
)

const (
	ContextLFSRequestKey = "lfs_request"
	ContextRequestIDKey  = "request_id"
)

var (
	contextLogMap = map[string]string{
		ContextLFSRequestKey: "r",
		ContextRequestIDKey:  "ri",
	}
)

func init() {
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&zt_formatter.ZtFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf("%s:%d", filename, f.Line)
		},
		Formatter: nested.Formatter{},
	})
}

func buildLogger(ctx context.Context) *logrus.Entry {
	fields := logrus.Fields{}
	for contextKey, logKey := range contextLogMap {
		if v := ctx.Value(contextKey); v != nil {
			fields[logKey] = v
		}
	}
	return logrus.WithFields(fields)
}

func log(ctx context.Context, level logrus.Level, msg string, kvs ...interface{}) {
	entry := buildLogger(ctx)
	for i := 0; i < len(kvs)/2; {
		key, value := kvs[2*i], kvs[2*i+1]
		if keyStr, ok := key.(string); ok {
			entry = entry.WithField(keyStr, value)
		}
		i += 1
	}
	entry.Log(level, msg)
}

func D(ctx context.Context, msg string, kvs ...interface{}) {
	log(ctx, logrus.DebugLevel, msg, kvs...)
}

func I(ctx context.Context, msg string, kvs ...interface{}) {
	log(ctx, logrus.InfoLevel, msg, kvs...)
}

func W(ctx context.Context, msg string, kvs ...interface{}) {
	log(ctx, logrus.WarnLevel, msg, kvs...)
}

func E(ctx context.Context, msg string, kvs ...interface{}) {
	log(ctx, logrus.ErrorLevel, msg, kvs...)
}

func P(ctx context.Context, msg string, kvs ...interface{}) {
	log(ctx, logrus.PanicLevel, msg, kvs...)
	panic(fmt.Sprint("msg=%v, kvs=%v", msg, kvs))
}

func If(ctx context.Context, msg string, args ...interface{}) {
	log(ctx, logrus.InfoLevel, fmt.Sprintf(msg, args...))
}

func Wf(ctx context.Context, msg string, args ...interface{}) {
	log(ctx, logrus.WarnLevel, fmt.Sprintf(msg, args...))
}

func Ef(ctx context.Context, msg string, args ...interface{}) {
	log(ctx, logrus.ErrorLevel, fmt.Sprintf(msg, args...))
}

func Pf(ctx context.Context, msg string, args ...interface{}) {
	log(ctx, logrus.PanicLevel, fmt.Sprintf(msg, args...))
	panic(fmt.Sprintf(msg, args...))
}
