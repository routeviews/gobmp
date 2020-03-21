package gobmpsrv

import (
	"fmt"
	"io"
	"net"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	kafka "github.com/sbezverk/gobmp/pkg/kafkaproducer"
	"github.com/sbezverk/gobmp/pkg/parser"
)

// BMPServer defines methods to manage BMP Server
type BMPServer interface {
	Start()
	Stop()
}

type bmpServer struct {
	intercept       bool
	kafkaProducer   kafka.KafkaProducer
	sourcePort      int
	destinationPort int
	incoming        net.Listener
	stop            chan struct{}
}

func (srv *bmpServer) Start() {
	// Starting bmp server server
	glog.Infof("Starting gobmp server on %s, intercept mode: %t\n", srv.incoming.Addr().String(), srv.intercept)
	go srv.server()
}

func (srv *bmpServer) Stop() {
	glog.Infof("Stopping gobmp server\n")
	close(srv.stop)
	return
}

func (srv *bmpServer) server() {
	for {
		client, err := srv.incoming.Accept()
		if err != nil {
			glog.Errorf("fail to accept client connection with error: %+v", err)
			continue
		}
		glog.V(5).Infof("client %+v accepted, calling bmpWorker", client.RemoteAddr())
		go srv.bmpWorker(client)
	}
}

func (srv *bmpServer) bmpWorker(client net.Conn) {
	defer client.Close()
	var server net.Conn
	var err error
	if srv.intercept {
		server, err = net.Dial("tcp", ":"+fmt.Sprintf("%d", srv.destinationPort))
		if err != nil {
			glog.Errorf("failed to connect to destination with error: %+v", err)
			return
		}
		defer server.Close()
		glog.V(5).Infof("connection to destination server %v established, start intercepting", server.RemoteAddr())
	}
	queue := make(chan []byte)
	pstop := make(chan struct{})
	// Starting parser per client with dedicated work queue
	go parser.Parser(queue, pstop)
	defer func() {
		glog.V(5).Infof("all done with client %+v", client.RemoteAddr())
		close(pstop)
	}()
	for {
		headerMsg := make([]byte, bmp.CommonHeaderLength)
		if _, err := io.ReadAtLeast(client, headerMsg, bmp.CommonHeaderLength); err != nil {
			glog.Errorf("fail to read from client %+v with error: %+v", client.RemoteAddr(), err)
			return
		}
		// Recovering common header first
		header, err := bmp.UnmarshalCommonHeader(headerMsg[:bmp.CommonHeaderLength])
		if err != nil {
			glog.Errorf("fail to recover BMP message Common Header with error: %+v", err)
			continue
		}
		// Allocating space for the message body
		msg := make([]byte, int(header.MessageLength)-bmp.CommonHeaderLength)
		if _, err := io.ReadFull(client, msg); err != nil {
			glog.Errorf("fail to read from client %+v with error: %+v", client.RemoteAddr(), err)
			return
		}

		fullMsg := make([]byte, int(header.MessageLength))
		copy(fullMsg, headerMsg)
		copy(fullMsg[bmp.CommonHeaderLength:], msg)
		// Sending information to the server only in intercept mode
		if srv.intercept {
			if _, err := server.Write(fullMsg); err != nil {
				glog.Errorf("fail to write to server %+v with error: %+v", server.RemoteAddr(), err)
				return
			}
		}
		queue <- fullMsg
	}
}

// NewBMPServer instantiates a new instance of BMP Server
func NewBMPServer(sPort, dPort int, intercept bool, kp kafka.KafkaProducer) (BMPServer, error) {
	incoming, err := net.Listen("tcp", fmt.Sprintf(":%d", sPort))
	if err != nil {
		glog.Errorf("fail to setup listener on port %d with error: %+v", sPort, err)
		return nil, err
	}
	bmp := bmpServer{
		stop:            make(chan struct{}),
		sourcePort:      sPort,
		destinationPort: dPort,
		intercept:       intercept,
		kafkaProducer:   kp,
		incoming:        incoming,
	}

	return &bmp, nil
}
