package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/gansidui/gotcp"
	"github.com/jiangzhx/go/handle"
	"github.com/jiangzhx/go/protocol"
	"net"
	"os"
	"os/signal"
	//	"runtime"
	"syscall"
	"time"
)

type Callback struct{}

func (this *Callback) OnConnect(c *gotcp.Conn) bool {
	addr := c.GetRawConn().RemoteAddr()
	c.PutExtraData(addr)
	log.Info("OnConnect:", addr)
	return true
}

func (this *Callback) OnMessageTest(c *gotcp.Conn, p gotcp.Packet) bool {
	//	respPacket := p.(*resp.RESPPacket)
	//	err := c.AsyncWritePacket(resp.NewRESPRESPPacket("+OK\r\n"), time.Second)
	log.Info("OnMessage")
	c.AsyncWritePacket(resp.NewRESPRESPPacket("+OK\r\n"), time.Second)

	return true
}

func (this *Callback) OnMessage(c *gotcp.Conn, p gotcp.Packet) bool {
	//	if p == nil {
	//		return false
	//	}
	respPacket := p.(*resp.RESPPacket)
	log.Debug("OnMessage:", respPacket)

	cmd := respPacket.GetCMD()
	args := respPacket.GetArgs()
	new := make([]interface{}, len(args))
	for i, v := range args {
		new[i] = interface{}(v)
	}

	_node := node.KeyToNode(string(args[0]))
	conn := _node.Redispool.Get()
	defer conn.Close()
	replay, err := conn.Do(string(cmd), new...)

	//	log.Debug("CMD:", string(cmd))
	//	log.Debug("ARGS:", args)
	//	log.Debug("ADDRESS:", _node.Address)
	if err != nil || replay == nil {
		log.Infof("CMD:%s    ARGS:%s    replay:%s    err:%s", cmd, args, replay, err)
	}
	//	log.Debug("err:", err)
	//	log.Debug("----------------------------")

	c.AsyncWritePacket(resp.NewRESPRESPPacket(replay), time.Second)

	return true
}

func (this *Callback) OnClose(c *gotcp.Conn) {
	log.Info("OnClose:", c.GetExtraData())
}

func main() {
	//	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	logger, _ := log.LoggerFromConfigAsFile("seelog.xml")
	log.ReplaceLogger(logger)

	// create a listener
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "0.0.0.0:3333")
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	// initialize server params
	config := &gotcp.Config{
		AcceptTimeout:          5 * time.Second,
		ReadTimeout:            240 * time.Second,
		WriteTimeout:           240 * time.Second,
		PacketSizeLimit:        2048,
		PacketSendChanLimit:    20,
		PacketReceiveChanLimit: 20,
	}
	srv := gotcp.NewServer(config, &Callback{}, &resp.RESPProtocol{})

	// start server
	go srv.Start(listener)
	log.Info("listening:", listener.Addr())

	// catch system signal
	chSig := make(chan os.Signal)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Signal: ", <-chSig)

	// stop server
	srv.Stop()
}

func checkError(err error) {
	if err != nil {
		log.Error(err)
	}
}
