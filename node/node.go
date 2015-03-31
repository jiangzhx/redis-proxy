package node

import (
	//	"fmt"
	log "github.com/cihub/seelog"
	"github.com/jiangzhx/redis-proxy/redis"
	//	"strconv"
	"time"
)

type Node struct {
	start     int
	end       int
	address   string
	redispool *redis.Pool
}

func NewNode(start int, end int, address string) *Node {
	return &Node{
		start:     start,
		end:       end,
		address:   address,
		redispool: newPool(address),
	}
}
func (node *Node) InRange(slot int) bool {
	if slot >= node.start && slot <= node.end {
		return true
	}
	return false
}

func (node *Node) Do(cmd []byte, args [][]byte) (reply interface{}, err error) {
	new := make([]interface{}, len(args))
	for i, v := range args {
		new[i] = interface{}(v)
	}

	conn := node.redispool.Get()
	defer conn.Close()
	replay, err := conn.Do(string(cmd), new...)
	if err != nil || replay == nil {
		log.Error("CMD:%s    ARGS:%s    replay:%s    err:%s", cmd, args, replay, err)
	}
	return replay, err
}

func (node *Node) Destory() {
	node.redispool.Close()
}

func newPool(address string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address)
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
