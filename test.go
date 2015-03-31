package main

import (
	"fmt"
	//	"github.com/jiangzhx/go/redis"
	"github.com/garyburd/redigo/redis"
	"time"
)

func newPool(server string) *redis.Pool {

	return &redis.Pool{
		MaxIdle:     80,
		MaxActive:   12000,
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
func set(conn redis.Conn) {
	defer conn.Close()
	for i := 0; i < 10000; i++ {

		replay, err := conn.Do("set", "click", "wahahha")
		if err != nil || replay == nil {
			fmt.Printf("replay:%s    err:%s\r\n", replay, err)
			break
		}
	}
}

func main() {
	s := time.Now().Local()

	pool := newPool("x00:7379")

	conn := pool.Get()

	for i := 0; i < 100; i++ {
		//		fmt.Println("poolsize:", pool.ActiveCount())

		go set(conn)
	}
	time.Sleep(time.Second * 30)
	e := time.Now().Local()
	fmt.Println(e.Unix() - s.Unix())
	//	t.Logf("size is %d \n", size)
}
