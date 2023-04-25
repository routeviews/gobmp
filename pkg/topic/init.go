package topic

import (
	"os"
	"os/signal"
	"syscall"

	_ "embed"
	"github.com/golang/glog"
)

var config *topicConfig

//go:embed default.yaml
var defaultConfig []byte

func init() {
	var tcfg topicConfig
	config = &tcfg
	config.routers = make([]routerConfig, 0)
	config.hup = make(chan os.Signal, 1)
	config.bye = make(chan int)
	config.UpdateData(defaultConfig)
	signal.Notify(config.hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-config.hup:
				glog.Infof("Got SIGHUP, reloading topic config")
				err := config.Update()
				if err != nil {
					glog.Warningf("topic config reload: %v", err)
				}
				Debug()
			case <-config.bye:
				return
			}
		}
	}()
}
