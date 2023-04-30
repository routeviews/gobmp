package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"net/http"
	_ "net/http/pprof"

	"github.com/arl/statsviz"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/dumper"
	"github.com/sbezverk/gobmp/pkg/filer"
	"github.com/sbezverk/gobmp/pkg/gobmpsrv"
	"github.com/sbezverk/gobmp/pkg/kafka"
	"github.com/sbezverk/gobmp/pkg/pub"
	"github.com/sbezverk/gobmp/pkg/topic"
	"github.com/sbezverk/tools"
)

var (
	dstPort     int
	srcPort     int
	perfPort    int
	kafkaSrv    string
	bmpRaw      bool
	intercept   string
	splitAF     string
	dump        string
	file        string
	adminId     string
	topicConfig string
)

func init() {
	// Let Go decide for itself what this should be.
	// runtime.GOMAXPROCS(1)
	flag.IntVar(&srcPort, "source-port", 5000, "port exposed to outside")
	flag.IntVar(&dstPort, "destination-port", 5050, "port openBMP is listening")
	flag.StringVar(&kafkaSrv, "kafka-server", "", "URL to access Kafka server")
	flag.StringVar(&topicConfig, "topic-config", "", "Kafka topics configuration file")
	flag.BoolVar(&bmpRaw, "bmp-raw", false, "also publish BMP RAW messages")
	flag.StringVar(&intercept, "intercept", "false", "When intercept set \"true\", all incomming BMP messges will be copied to TCP port specified by destination-port, otherwise received BMP messages will be published to Kafka.")
	flag.StringVar(&splitAF, "split-af", "true", "When set \"true\" (default) ipv4 and ipv6 will be published in separate topics. if set \"false\" the same topic will be used for both address families.")
	flag.IntVar(&perfPort, "performance-port", 56767, "port used for performance debugging")
	flag.StringVar(&dump, "dump", "", "Dump resulting messages to file when \"dump=file\" or to the standard output when \"dump=console\"")
	flag.StringVar(&file, "msg-file", "/tmp/messages.json", "Full path anf file name to store messages when \"dump=file\"")
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "dummy"
	}
	flag.StringVar(&adminId, "admin-id", hostname, "This collector's admin id (used in \"bmp-raw\" mode)")
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")

	if topicConfig != "" {
		topic.SetConfigLocation(topicConfig)
		err := topic.UpdateConfig()
		if err != nil {
			glog.Errorf("loading topic config from \"%s\": %v", topicConfig, err)
			os.Exit(1)
		}
	}

	statsviz.RegisterDefault()
	// Starting performance collecting http server
	go func() {
		glog.Info(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", perfPort), nil))
	}()
	// Initializing publisher
	var publisher pub.Publisher
	var err error
	switch strings.ToLower(dump) {
	case "file":
		publisher, err = filer.NewFiler(file)
		if err != nil {
			glog.Errorf("failed to initialize file publisher with error: %+v", err)
			os.Exit(1)
		}
		glog.V(5).Infof("file publisher has been successfully initialized.")
	case "console":
		publisher, err = dumper.NewDumper()
		if err != nil {
			glog.Errorf("failed to initialize console publisher with error: %+v", err)
			os.Exit(1)
		}
		glog.V(5).Infof("console publisher has been successfully initialized.")
	default:
		publisher, err = kafka.NewKafkaPublisher(kafkaSrv)
		if err != nil {
			glog.Errorf("failed to initialize Kafka publisher with error: %+v", err)
			os.Exit(1)
		}
		glog.V(5).Infof("Kafka publisher has been successfully initialized.")
	}

	// Initializing bmp server
	interceptFlag, err := strconv.ParseBool(intercept)
	if err != nil {
		glog.Errorf("failed to parse to bool the value of the intercept flag with error: %+v", err)
		os.Exit(1)
	}
	splitAFFlag, err := strconv.ParseBool(splitAF)
	if err != nil {
		glog.Errorf("failed to parse to bool the value of the intercept flag with error: %+v", err)
		os.Exit(1)
	}
	bmpSrv, err := gobmpsrv.NewBMPServer(srcPort, dstPort, interceptFlag, publisher, splitAFFlag, bmpRaw, adminId)
	if err != nil {
		glog.Errorf("failed to setup new gobmp server with error: %+v", err)
		os.Exit(1)
	}
	// Starting Interceptor server
	bmpSrv.Start()

	stopCh := tools.SetupSignalHandler()
	<-stopCh

	bmpSrv.Stop()
	topic.Cleanup()
	os.Exit(0)
}
