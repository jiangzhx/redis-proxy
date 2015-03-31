package redis

import (
	"github.com/jiangzhx/go/redis"
	"testing"
	"time"
)

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
		//		TestOnBorrow: func(c redis.Conn, t time.Time) error {
		//			_, err := c.Do("PING")
		//			return err
		//		},
	}
}
func TestConn(t *testing.T) {

	conn, _ := redis.DialTimeout("tcp", "x00:6379", 0, 1*time.Second, 1*time.Second)
	size, _ := conn.Do("DBSIZE")
	t.Logf("size is %d \n", size)
}
func TestSet(t *testing.T) {

	conn, _ := redis.DialTimeout("tcp", "x00:6379", 0, 1*time.Second, 1*time.Second)
	replay, _ := conn.Do("set", "click", "wahahha")
	t.Log(replay)
	t.Log([]byte("+OK\r\n"))
	t.Logf("replay is %s \n", replay)
}

func set(conn redis.Conn, t *testing.T) {
	defer conn.Close()
	for i := 0; i < 10000; i++ {
		t.Log(i)
		replay, err := conn.Do("set", "click", "wahahha")
		if err != nil || replay == nil {
			t.Fatalf("replay:%s    err:%s", replay, err)
		}
	}
}

func TestMultThread(t *testing.T) {
	pool := newPool("x00:7379", "")
	conn := pool.Get()

	for i := 0; i < 50; i++ {
		t.Log(i)
		go set(conn, t)
	}
	time.Sleep(time.Second * 30)
	//	t.Logf("size is %d \n", size)
}

func TestGet(t *testing.T) {

	conn, _ := redis.DialTimeout("tcp", "x00:6379", 0, 1*time.Second, 1*time.Second)
	replay, _ := conn.Do("get", "click")
	t.Logf("replay is %s \n", replay)
}
func TestHGetAll(t *testing.T) {

	conn, _ := redis.DialTimeout("tcp", "x00:6379", 0, 1*time.Second, 1*time.Second)
	replay, _ := conn.Do("hgetall", "channel")
	t.Logf("replay is %s \n", replay)
}
func TestPool(t *testing.T) {

	pool := newPool("x00:7379", "")
	conn := pool.Get()
	size, _ := conn.Do("dbsize")
	t.Logf("size is %d \n", size)
}
