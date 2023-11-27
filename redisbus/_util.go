package redisbus

// NOTE: we do not use redigo/redis package anymore.

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
)

func getRedisDBIndex(conn *redis.Pool) (int, error) {
	c := conn.Get()
	defer c.Close()

	reply, err := c.Do("CLIENT", "INFO")
	if err != nil {
		return 0, err
	}

	data := reply.([]byte)

	keys := strings.Split(string(data), " ")
	for _, k := range keys {
		if strings.Index(k, "db=") == 0 {
			v := strings.Split(k, "=")
			if len(v) != 2 {
				return 0, fmt.Errorf("unable to parse redis db index")
			}
			id, err := strconv.ParseInt(v[1], 10, 32)
			if err != nil {
				return 0, fmt.Errorf("unable to parse redis db index")
			}
			return int(id), nil
		}
	}

	return 0, fmt.Errorf("unable to parse redis db index")
}
