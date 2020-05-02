package utility

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/Berailitz/pfs/logger"
)

type CMDParam struct {
	CMD string `yaml:"cmd,omitempty"`
	LOG string `yaml:"log,omitempty"`
}

func RunCMD(ctx context.Context, wg *sync.WaitGroup, cparam CMDParam) (err error) {
	cmdText := cparam.CMD
	logPath := cparam.LOG
	logger.If(ctx, "run cmd: cmd=%v, logPath=%v", cmdText, logPath)
	defer func() {
		logger.If(ctx, "cmd finished success: cmd=%v, logPath=%v", cmdText, logPath)
		wg.Done()
	}()

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		logger.Pf(ctx, "open log error: logPath=%v", logPath)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			logger.Ef(ctx, "close log error: logPath=%v, cerr=%+v", logPath, cerr)
			if err == nil {
				err = cerr
			}
		}
	}()
	mwriter := io.MultiWriter(os.Stdout, f)

	cmd := exec.Command("bash", "-c", cmdText)
	cmd.Stdout = mwriter
	cmd.Stderr = mwriter

	if err = cmd.Start(); err != nil {
		logger.Pf(ctx, "start script error: err=%+v", err)
		return err
	}
	logger.If(ctx, "cmd start success: cmd=%v, logPath=%v", cmdText, logPath)

	if err = cmd.Wait(); err != nil {
		logger.Ef(ctx, "wait cmd error: cmd=%v, logPath=%v, err=%+v", cmdText, logPath, err)
		return err
	}
	return nil
}
