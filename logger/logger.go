package logger

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"

	"google.golang.org/grpc/codes"

	zt_formatter "github.com/Berailitz/pfs/logger/formatter"
	"github.com/Berailitz/pfs/logger/lfshook"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
)

const (
	ContextRequestIDKey = "request_id"
	// frameToSkip is updated if func name is incorrect
	frameToSkip   = 5
	maxStackDepth = 20
)

const (
	logPathTpl = "log/%v.log"
)

const (
	logGRPCErrorKey      = "ge"
	logGRPCCodeKey       = "gc"
	logGRPCRequestKey    = "req"
	logGRPCResponseKey   = "res"
	logGRPCMethodKey     = "gm"
	contextGRPCMethodKey = "grpc.method"
)

var (
	contextLogMap = map[string]string{
		ContextRequestIDKey: "ri",
	}
	loggerPackageNames = []string{"logger", "logrus"}
)

var (
	entry *logrus.Entry = nil
)

func getPackageName(name string) string {
	for {
		lastPeriod := strings.LastIndex(name, ".")
		lastSlash := strings.LastIndex(name, "/")
		if lastPeriod > lastSlash {
			name = name[:lastPeriod]
		} else {
			break
		}
	}
	return name
}

func init() {
	ctx := context.Background()
	logPathMap := lfshook.PathMap{
		logrus.DebugLevel: fmt.Sprintf(logPathTpl, "d"),
		logrus.InfoLevel:  fmt.Sprintf(logPathTpl, "i"),
		logrus.WarnLevel:  fmt.Sprintf(logPathTpl, "w"),
		logrus.ErrorLevel: fmt.Sprintf(logPathTpl, "e"),
	}
	logConsleMap := lfshook.WriterMap{
		logrus.InfoLevel:  os.Stdout,
		logrus.WarnLevel:  os.Stdout,
		logrus.ErrorLevel: os.Stdout,
	}
	formatter := &zt_formatter.ZtFormatter{
		CallerPrettyfier: func(_ *runtime.Frame) (string, string) {
			pc := make([]uintptr, maxStackDepth)
			n := runtime.Callers(frameToSkip, pc)
			for i := 0; i < n; i++ {
				frames := runtime.CallersFrames(pc[i:n])
				f, _ := frames.Next()
				packageName := getPackageName(f.Function)
				isOutOfLogger := true
				for _, loggerPackageName := range loggerPackageNames {
					if pos := strings.LastIndex(packageName, loggerPackageName); pos != -1 {
						isOutOfLogger = false
					}
				}

				if isOutOfLogger {
					lastSlash := strings.LastIndexByte(f.Function, '/')
					return fmt.Sprintf("%d", f.Line), f.Function[lastSlash+1:]
				}
			}
			return "", ""
		},
		Formatter: nested.Formatter{
			TimestampFormat: "15:04:05.000",
		},
	}
	logrus.SetReportCaller(true)
	logrus.SetFormatter(formatter)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(ioutil.Discard)

	logDir := path.Dir(logPathTpl)
	if err := os.MkdirAll(logDir, 0644); err != nil {
		P(ctx, "mkdir log dir error", "logDir", logDir, "logPathTpl", logPathTpl)
	}

	logrus.AddHook(lfshook.NewHook(
		logConsleMap,
		formatter,
	))
	logrus.AddHook(lfshook.NewHook(
		logPathMap,
		formatter,
	))
	entry = logrus.NewEntry(logrus.New())
}

func Entry() *logrus.Entry {
	return entry
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

func StubMessageProducer(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields, req, resp interface{}) {
	if err != nil {
		fields[logGRPCErrorKey] = err
	}
	fields[logGRPCRequestKey] = req
	fields[logGRPCResponseKey] = resp
	fields[logGRPCMethodKey] = ctx.Value(contextGRPCMethodKey)
	fields[logGRPCCodeKey] = code

	entry := buildLogger(ctx).WithFields(fields)
	switch level {
	case logrus.DebugLevel:
		entry.Debugf(format)
	case logrus.InfoLevel:
		entry.Infof(format)
	case logrus.WarnLevel:
		entry.Warningf(format)
	case logrus.ErrorLevel:
		entry.Errorf(format)
	case logrus.FatalLevel:
		entry.Fatalf(format)
	case logrus.PanicLevel:
		entry.Panicf(format)
	}
}
