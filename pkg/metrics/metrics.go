package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type IncrementorDecrementor interface {
}

var Metrics struct {
	BMPConnections            *prometheus.GaugeVec
	GoroutineProducers        prometheus.Gauge
	GoroutineProducingWorkers prometheus.Gauge
}

func init() {
	Metrics.BMPConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gobmp_bmp_connections",
		Help: "BMP client connections",
	}, []string{"client"})

	Metrics.GoroutineProducers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gobmp_goroutine_producers",
		Help: "Producer goroutines",
	})

	Metrics.GoroutineProducingWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gobmp_goroutine_producing_workers",
		Help: "Producing worker goroutines",
	})

	//	Metrics.UPDPConnAnnouncementsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
	//	        Name: "gorib_announcements_received",
	//	        Help: "The number of received announcement or rib entries for a collector",
	//	}, []string{"collector"})
}

// 81:     Info.AnnouncementsReceived.Inc()
// 82:     Info.UPDPConnAnnouncementsReceived.WithLabelValues(el.Peer.Collector.Name).Inc()
