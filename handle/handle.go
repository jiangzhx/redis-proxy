package node

import (
	"fmt"
	//	log "github.com/cihub/seelog"
	ketama "github.com/dgryski/go-ketama"
	"github.com/jiangzhx/redis-proxy/redis"
	"strconv"
	"time"
)

var (
	nodes = initNodes()
	k, _  = ketama.New(initSlots(1024))
)

type Node struct {
	Start     int
	End       int
	Address   string
	Redispool *redis.Pool
}

func GetNodeActivePoolSize() {
	for _, node := range nodes {
		fmt.Println(node.Redispool.ActiveCount())
	}
}

func initSlots(size int) []ketama.Bucket {
	var buckets []ketama.Bucket
	for i := 1; i <= size; i++ {
		b := &ketama.Bucket{Label: fmt.Sprintf("%d", i), Weight: 1}
		buckets = append(buckets, *b)
	}
	return buckets
}

func initNodes() []Node {
	var nodes []Node
	nodes = append(nodes, Node{Start: 1, End: 512, Address: "x01:7380", Redispool: newPool("x01:7380", "")})
	nodes = append(nodes, Node{Start: 513, End: 1024, Address: "x01:7380", Redispool: newPool("x01:7380", "")})
	return nodes
}

func KeyToNode(key string) Node {
	slot, _ := strconv.Atoi(k.Hash(key))
	for i := 0; i < len(nodes); i++ {
		if slot >= nodes[i].Start && slot <= nodes[i].End {
			return nodes[i]
		}
	}
	return Node{}

}
func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func Redirect(cmd []byte, args [][]byte) (reply interface{}, err error) {
	new := make([]interface{}, len(args))
	for i, v := range args {
		new[i] = interface{}(v)
	}
	_node := KeyToNode(string(args[0]))
	conn := _node.Redispool.Get()
	defer conn.Close()
	replay, err := conn.Do(string(cmd), new...)
	//	log.Debug("CMD:", string(cmd))
	//	log.Debug("ARGS:", args)
	//	log.Debug("ADDRESS:", _node.Address)
	return replay, err
}
