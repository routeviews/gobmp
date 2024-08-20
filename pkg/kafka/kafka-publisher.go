package kafka

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/pub"
	"github.com/sbezverk/gobmp/pkg/topic"
)

var (
	brockerConnectTimeout = 10 * time.Second
	topicCreateTimeout    = 1 * time.Second
	// goBMP topic's retention timer is 15 minutes
	topicRetention = "900000"
	// try to re-create a given topic after 5 minutes have passed
	topicRenew = 300.0
)

type publisher struct {
	broker          *sarama.Broker
	config          *sarama.Config
	producer        sarama.AsyncProducer
	stopCh          chan struct{}
	topicCacheSeen  map[string]time.Time
	topicCacheUses  map[string]uint64
	topicCacheMutex sync.Mutex
}

func (p *publisher) useTopic(topicType string, vars map[string]string) (string, error) {
	t := topic.GetTopic(topicType, vars)
	if t == "" {
		return t, nil
	}
	p.topicCacheMutex.Lock()
	defer p.topicCacheMutex.Unlock()
	if time.Since(p.topicCacheSeen[t]).Seconds() > topicRenew {
		glog.V(5).Infof("(re)creating topic %#v (%v uses since last)\n", t, p.topicCacheUses[t])
		if err := ensureTopic(p.broker, topicCreateTimeout, t); err != nil {
			glog.Errorf("Kafka publisher failed to ensure topic %#v with error: %+v", t, err)
			return "", err
		}
		p.topicCacheSeen[t] = time.Now()
		p.topicCacheUses[t] = 0
	}
	p.topicCacheUses[t]++
	return t, nil
}

func (p *publisher) PublishMessage(t int, vars map[string]string, key []byte, msg []byte) error {
	var topicName string
	var err error
	switch t {
	case bmp.BMPRawMsg:
		topicName, err = p.useTopic("raw", vars)
	case bmp.PeerStateChangeMsg:
		topicName, err = p.useTopic("peer", vars)
	case bmp.UnicastPrefixMsg:
		topicName, err = p.useTopic("unicast_prefix", vars)
	case bmp.UnicastPrefixV4Msg:
		topicName, err = p.useTopic("unicast_prefix_v4", vars)
	case bmp.UnicastPrefixV6Msg:
		topicName, err = p.useTopic("unicast_prefix_v6", vars)
	case bmp.LSNodeMsg:
		topicName, err = p.useTopic("ls_node", vars)
	case bmp.LSLinkMsg:
		topicName, err = p.useTopic("ls_link", vars)
	case bmp.L3VPNMsg:
		topicName, err = p.useTopic("l3vpn", vars)
	case bmp.L3VPNV4Msg:
		topicName, err = p.useTopic("l3vpn_v4", vars)
	case bmp.L3VPNV6Msg:
		topicName, err = p.useTopic("l3vpn_v6", vars)
	case bmp.LSPrefixMsg:
		topicName, err = p.useTopic("ls_prefix", vars)
	case bmp.LSSRv6SIDMsg:
		topicName, err = p.useTopic("ls_srv6_sid", vars)
	case bmp.EVPNMsg:
		topicName, err = p.useTopic("evpn", vars)
	case bmp.SRPolicyMsg:
		topicName, err = p.useTopic("sr_policy", vars)
	case bmp.SRPolicyV4Msg:
		topicName, err = p.useTopic("sr_policy_v4", vars)
	case bmp.SRPolicyV6Msg:
		topicName, err = p.useTopic("sr_policy_v6", vars)
	case bmp.FlowspecMsg:
		topicName, err = p.useTopic("flowspec", vars)
	case bmp.FlowspecV4Msg:
		topicName, err = p.useTopic("flowspec_v4", vars)
	case bmp.FlowspecV6Msg:
		topicName, err = p.useTopic("flowspec_v6", vars)
	case bmp.StatsReportMsg:
		topicName, err = p.useTopic("stats", vars)
	}
	if err != nil {
		return err
	}
	// don't produce anything if topicName is empty
	if topicName == "" {
		return nil
	}
	return p.produceMessage(topicName, key, msg)
}

func (p *publisher) produceMessage(topic string, key []byte, msg []byte) error {
	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(msg),
	}

	return nil
}

func (p *publisher) Stop() {
	close(p.stopCh)
	p.broker.Close()
}

// NewKafkaPublisher instantiates a new instance of a Kafka publisher
func NewKafkaPublisher(kafkaSrv string) (pub.Publisher, error) {
	glog.Infof("Initializing Kafka producer client")
	if err := validator(kafkaSrv); err != nil {
		glog.Errorf("Failed to validate Kafka server address %s with error: %+v", kafkaSrv, err)
		return nil, err
	}
	config := sarama.NewConfig()
	config.ClientID = "gobmp-producer" + "_" + strconv.Itoa(rand.Intn(1000))
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_11_0_0

	br := sarama.NewBroker(kafkaSrv)
	if err := br.Open(config); err != nil {
		if err != sarama.ErrAlreadyConnected {
			return nil, err
		}
	}
	if err := waitForBrokerConnection(br, brockerConnectTimeout); err != nil {
		glog.Errorf("failed to open connection to the broker with error: %+v\n", err)
		return nil, err
	}
	glog.V(5).Infof("Connected to broker: %s id: %d\n", br.Addr(), br.ID())

	producer, err := sarama.NewAsyncProducer([]string{kafkaSrv}, config)
	if err != nil {
		glog.Errorf("New Kafka publisher failed to start new async producer with error: %+v", err)
		return nil, err
	}
	glog.V(5).Infof("Initialized Kafka Async producer")
	stopCh := make(chan struct{})
	go func(producer sarama.AsyncProducer, stopCh <-chan struct{}) {
		for {
			select {
			case <-producer.Successes():
			case err := <-producer.Errors():
				glog.Errorf("failed to produce message with error: %+v", *err)
			case <-stopCh:
				producer.Close()
				return
			}
		}
	}(producer, stopCh)

	return &publisher{
		stopCh:         stopCh,
		broker:         br,
		config:         config,
		producer:       producer,
		topicCacheSeen: make(map[string]time.Time),
		topicCacheUses: make(map[string]uint64),
	}, nil
}

func validator(addr string) error {
	host, port, _ := net.SplitHostPort(addr)
	if host == "" || port == "" {
		return fmt.Errorf("host or port cannot be ''")
	}
	// Try to resolve if the hostname was used in the address
	if ip, err := net.LookupIP(host); err != nil || ip == nil {
		// Check if IP address was used in address instead of a host name
		if net.ParseIP(host) == nil {
			return fmt.Errorf("fail to parse host part of address")
		}
	}
	np, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("fail to parse port with error: %w", err)
	}
	if np == 0 || np > math.MaxUint16 {
		return fmt.Errorf("the value of port is invalid")
	}
	return nil
}

func ensureTopic(br *sarama.Broker, timeout time.Duration, topicName string) error {
	topic := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topicName: {
				NumPartitions:     1,
				ReplicationFactor: 1,
				ConfigEntries: map[string]*string{
					"retention.ms": &topicRetention,
				},
			},
		},
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	tout := time.NewTimer(timeout)
	for {
		t, err := br.CreateTopics(topic)
		if err != nil {
			return err
		}
		if e, ok := t.TopicErrors[topicName]; ok {
			if e.Err == sarama.ErrTopicAlreadyExists || e.Err == sarama.ErrNoError {
				return nil
			}
			if e.Err != sarama.ErrRequestTimedOut {
				return e
			}
		}
		select {
		case <-ticker.C:
			continue
		case <-tout.C:
			return fmt.Errorf("timeout waiting for topic %s", topicName)
		}
	}
}

func waitForBrokerConnection(br *sarama.Broker, timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	tout := time.NewTimer(timeout)
	for {
		ok, err := br.Connected()
		if ok {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ticker.C:
			continue
		case <-tout.C:
			return fmt.Errorf("timeout waiting for the connection to the broker %s", br.Addr())
		}
	}

}
