package utility

import (
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
)

type CMDParam struct {
	CMD string `yaml:"cmd,omitempty"`
	LOG string `yaml:"log,omitempty"`
}

func RunCMD(wg *sync.WaitGroup, cparam CMDParam) (err error) {
	cmdText := cparam.CMD
	logPath := cparam.LOG
	log.Printf("run cmd: cmd=%v, logPath=%v", cmdText, logPath)
	defer func() {
		log.Printf("cmd finished success: cmd=%v, logPath=%v", cmdText, logPath)
		wg.Done()
	}()

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("open log error: logPath=%v", logPath)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Printf("close log error: logPath=%v, cerr=%+v", logPath, cerr)
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
		log.Fatalf("start script error: err=%+v", err)
		return err
	}
	log.Printf("cmd start success: cmd=%v, logPath=%v", cmdText, logPath)

	if err = cmd.Wait(); err != nil {
		log.Printf("wait cmd error: cmd=%v, logPath=%v, err=%+v", cmdText, logPath, err)
		return err
	}
	return nil
}
