package kafka

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/pub"
)

// Define constants for each topic name
const (
	rawTopic               = "gobmp.bmp_raw"
	peerTopic              = "gobmp.parsed.peer"
	unicastMessageTopic    = "gobmp.parsed.unicast_prefix"
	unicastMessageV4Topic  = "gobmp.parsed.unicast_prefix_v4"
	unicastMessageV6Topic  = "gobmp.parsed.unicast_prefix_v6"
	lsNodeMessageTopic     = "gobmp.parsed.ls_node"
	lsLinkMessageTopic     = "gobmp.parsed.ls_link"
	l3vpnMessageTopic      = "gobmp.parsed.l3vpn"
	l3vpnMessageV4Topic    = "gobmp.parsed.l3vpn_v4"
	l3vpnMessageV6Topic    = "gobmp.parsed.l3vpn_v6"
	lsPrefixMessageTopic   = "gobmp.parsed.ls_prefix"
	lsSRv6SIDMessageTopic  = "gobmp.parsed.ls_srv6_sid"
	evpnMessageTopic       = "gobmp.parsed.evpn"
	srPolicyMessageTopic   = "gobmp.parsed.sr_policy"
	srPolicyMessageV4Topic = "gobmp.parsed.sr_policy_v4"
	srPolicyMessageV6Topic = "gobmp.parsed.sr_policy_v6"
	flowspecMessageTopic   = "gobmp.parsed.flowspec"
	flowspecMessageV4Topic = "gobmp.parsed.flowspec_v4"
	flowspecMessageV6Topic = "gobmp.parsed.flowspec_v6"
	statsMessageTopic      = "gobmp.parsed.statistics"
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
	topicCache      map[string]time.Time
	topicCacheMutex sync.Mutex
}

func (p *publisher) useTopic(topicType string, vars map[string]string) (string, error) {
	t := topicType
	p.topicCacheMutex.Lock()
	defer p.topicCacheMutex.Unlock()
	if time.Since(p.topicCache[t]).Seconds() > topicRenew {
		glog.V(5).Infof("(re)creating topic %#v\n", t)
		if err := ensureTopic(p.broker, topicCreateTimeout, t); err != nil {
			glog.Errorf("Kafka publisher failed to ensure topic %#v with error: %+v", t, err)
			return "", err
		}
		p.topicCache[t] = time.Now()
	}
	return t, nil
}

func (p *publisher) PublishMessage(t int, vars map[string]string, key []byte, msg []byte) error {
	var topicName string
	var err error
	switch t {
	case bmp.BMPRawMsg:
		topicName, err = p.useTopic(rawTopic, vars)
	case bmp.PeerStateChangeMsg:
		topicName, err = p.useTopic(peerTopic, vars)
	case bmp.UnicastPrefixMsg:
		topicName, err = p.useTopic(unicastMessageTopic, vars)
	case bmp.UnicastPrefixV4Msg:
		topicName, err = p.useTopic(unicastMessageV4Topic, vars)
	case bmp.UnicastPrefixV6Msg:
		topicName, err = p.useTopic(unicastMessageV6Topic, vars)
	case bmp.LSNodeMsg:
		topicName, err = p.useTopic(lsNodeMessageTopic, vars)
	case bmp.LSLinkMsg:
		topicName, err = p.useTopic(lsLinkMessageTopic, vars)
	case bmp.L3VPNMsg:
		topicName, err = p.useTopic(l3vpnMessageTopic, vars)
	case bmp.L3VPNV4Msg:
		topicName, err = p.useTopic(l3vpnMessageV4Topic, vars)
	case bmp.L3VPNV6Msg:
		topicName, err = p.useTopic(l3vpnMessageV6Topic, vars)
	case bmp.LSPrefixMsg:
		topicName, err = p.useTopic(lsPrefixMessageTopic, vars)
	case bmp.LSSRv6SIDMsg:
		topicName, err = p.useTopic(lsSRv6SIDMessageTopic, vars)
	case bmp.EVPNMsg:
		topicName, err = p.useTopic(evpnMessageTopic, vars)
	case bmp.SRPolicyMsg:
		topicName, err = p.useTopic(srPolicyMessageTopic, vars)
	case bmp.SRPolicyV4Msg:
		topicName, err = p.useTopic(srPolicyMessageV4Topic, vars)
	case bmp.SRPolicyV6Msg:
		topicName, err = p.useTopic(srPolicyMessageV6Topic, vars)
	case bmp.FlowspecMsg:
		topicName, err = p.useTopic(flowspecMessageTopic, vars)
	case bmp.FlowspecV4Msg:
		topicName, err = p.useTopic(flowspecMessageV4Topic, vars)
	case bmp.FlowspecV6Msg:
		topicName, err = p.useTopic(flowspecMessageV6Topic, vars)
	case bmp.StatsReportMsg:
		topicName, err = p.useTopic(statsMessageTopic, vars)
	}
	if err != nil {
		return err
	}
	if topicName == "" {
		return fmt.Errorf("empty topic")
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
		stopCh:     stopCh,
		broker:     br,
		config:     config,
		producer:   producer,
		topicCache: make(map[string]time.Time),
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
