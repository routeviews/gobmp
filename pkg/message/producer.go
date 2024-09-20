package message

import (
	"crypto/md5"
	"fmt"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/metrics"
	"github.com/sbezverk/gobmp/pkg/pub"
)

const (
	peerUP = iota
	peerDown
)

const ProducerConcurrency = 512

// Producer defines methods to act as a message producer
type Producer interface {
	Producer(queue chan bmp.Message, stop chan struct{})
}

type producer struct {
	publisher      pub.Publisher
	speakerIP      string
	speakerHash    string
	addPathCapable map[int]bool
	adminHash      string
	// If splitAF is set to true, ipv4 and ipv6 messages will go into separate topics
	splitAF bool
}

// Producer dispatches kafka workers upon request received from the channel
func (p *producer) Producer(queue chan bmp.Message, stop chan struct{}) {
	metrics.Metrics.GoroutineProducers.Inc()
	defer metrics.Metrics.GoroutineProducers.Dec()

	funnel := make(chan int, ProducerConcurrency)
	for {
		select {
		case msg := <-queue:
			// XXX throttle point?
			funnel <- 1
			go func() {
				p.producingWorker(msg)
				<-funnel
			}()
		case <-stop:
			glog.Infof("received interrupt, stopping.")
			return
		}
	}
}

func (p *producer) producingWorker(msg bmp.Message) {
	metrics.Metrics.GoroutineProducingWorkers.Inc()
	defer metrics.Metrics.GoroutineProducingWorkers.Dec()

	switch obj := msg.Payload.(type) {
	case *bmp.PeerUpMessage:
		p.producePeerMessage(peerUP, msg)
	case *bmp.PeerDownMessage:
		p.producePeerMessage(peerDown, msg)
	case *bmp.RouteMonitor:
		p.produceRouteMonitorMessage(msg)
	case *bmp.StatsReport:
		p.produceStatsMessage(msg)
	case *bmp.RawMessage:
		p.produceRawMessage(msg)
	default:
		glog.Warningf("got Unknown message %T to push to the producer, ignoring it...", obj)
	}
}

// NewProducer instantiates a new instance of a producer with Publisher interface
func NewProducer(publisher pub.Publisher, adminId string, splitAF bool) Producer {
	data := []byte{}
	data = append(data, []byte(adminId)...)

	return &producer{
		publisher:      publisher,
		splitAF:        splitAF,
		addPathCapable: make(map[int]bool),
		adminHash:      fmt.Sprintf("%x", md5.Sum(data)),
	}
}
