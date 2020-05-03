package logger

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"

	zt_formatter "github.com/Berailitz/pfs/logger/formatter"
	"github.com/Berailitz/pfs/logger/lfshook"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
)

const (
	ContextRequestIDKey = "request_id"
	// frameToSkip is updated if func name is incorrect
	frameToSkip = 10
)

const (
	logPathPrefix = "log/pfs.log"
)

var (
	contextLogMap = map[string]string{
		ContextRequestIDKey: "ri",
	}
)

func init() {
	ctx := context.Background()
	logPathMap := lfshook.PathMap{
		logrus.DebugLevel: fmt.Sprintf("%v.%v", logPathPrefix, "debug"),
		logrus.InfoLevel:  fmt.Sprintf("%v.%v", logPathPrefix, "info"),
		logrus.ErrorLevel: fmt.Sprintf("%v.%v", logPathPrefix, "error"),
	}
	formatter := &zt_formatter.ZtFormatter{
		CallerPrettyfier: func(_ *runtime.Frame) (string, string) {
			pc := make([]uintptr, 1)
			n := runtime.Callers(frameToSkip, pc)
			frames := runtime.CallersFrames(pc[:n])
			f, _ := frames.Next()
			lastSlash := strings.LastIndexByte(f.Function, '/')
			return fmt.Sprintf("%d", f.Line), f.Function[lastSlash+1:]
		},
		Formatter: nested.Formatter{
			TimestampFormat: "15:04:05.000",
		},
	}
	logrus.SetReportCaller(true)
	logrus.SetFormatter(formatter)

	logDir := path.Dir(logPathPrefix)
	if err := os.MkdirAll(logDir, 0644); err != nil {
		P(ctx, "mkdir log dir error", "logDir", logDir, "logPathPrefix", logPathPrefix)
	}

	logrus.AddHook(lfshook.NewHook(
		logPathMap,
		formatter,
	))
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
