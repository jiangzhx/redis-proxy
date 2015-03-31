package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/gansidui/gotcp"
	//	"github.com/jiangzhx/redis-proxy/handle"
	ketama "github.com/dgryski/go-ketama"
	"github.com/jiangzhx/redis-proxy/node"
	"github.com/jiangzhx/redis-proxy/protocol"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

var (
	k, _ = ketama.New(initSlots(1024))
)

type Callback struct {
	//	nodes   []*node.Node
	clients map[string][]*node.Node
}

func getAddrWithString(c *gotcp.Conn) string {
	addr := c.GetRawConn().RemoteAddr()
	return fmt.Sprint("%s", addr)
}

func initSlots(size int) []ketama.Bucket {
	var buckets []ketama.Bucket
	for i := 1; i <= size; i++ {
		b := &ketama.Bucket{Label: fmt.Sprintf("%d", i), Weight: 1}
		buckets = append(buckets, *b)
	}
	return buckets
}
func keyToNode(key string, nodes []*node.Node) *node.Node {
	slot, _ := strconv.Atoi(k.Hash(key))
	for _, node := range nodes {
		if node.InRange(slot) {
			return node
		}
	}

	return &node.Node{}

}

func (this *Callback) OnConnect(c *gotcp.Conn) bool {
	addr := c.GetRawConn().RemoteAddr()
	c.PutExtraData(addr)

	var nodes []*node.Node
	nodes = append(nodes, node.NewNode(1, 512, "x00:7379"))
	nodes = append(nodes, node.NewNode(513, 1024, "x01:7380"))

	if this.clients == nil {
		this.clients = make(map[string][]*node.Node)
	}

	this.clients[getAddrWithString(c)] = nodes

	log.Debugf("init %d num node to use", len(nodes))

	log.Debug("OnConnect:", addr)
	return true
}

func (this *Callback) OnMessage(c *gotcp.Conn, p gotcp.Packet) bool {

	respPacket := p.(*resp.RESPPacket)
	log.Debug("OnMessage:", respPacket)

	cmd := respPacket.GetCMD()
	args := respPacket.GetArgs()

	nodes := this.clients[getAddrWithString(c)]
	node := keyToNode(string(args[0]), nodes)
	replay, _ := node.Do(cmd, args)

	if replay != nil {
		c.AsyncWritePacket(resp.NewRESPRESPPacket(replay), time.Second)
	}

	return true
}

func (this *Callback) OnClose(c *gotcp.Conn) {
	nodes := this.clients[getAddrWithString(c)]
	for _, node := range nodes {
		node.Destory()
	}
	delete(this.clients, getAddrWithString(c))

	log.Debug("OnClose:", c.GetExtraData())
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
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
