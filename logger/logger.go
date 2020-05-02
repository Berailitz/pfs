package logger

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
	"github.com/zput/zxcTool/ztLog/zt_formatter"
)

const (
	ContextRequestIDKey = "request_id"
)

var (
	contextLogMap = map[string]string{
		ContextRequestIDKey: "ri",
	}
)

func init() {
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&zt_formatter.ZtFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			lastSlash := strings.LastIndexByte(f.Function, '/')
			if lastSlash < 0 {
				lastSlash = 0
			}
			return fmt.Sprintf("%d", f.Line), f.Function[lastSlash+1:]
		},
		Formatter: nested.Formatter{
			TimestampFormat: "15:04:05.000",
		},
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
