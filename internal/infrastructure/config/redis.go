package config

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ctx     = context.Background()
	mu      sync.Mutex
	clients = make(map[string]*redis.Client) // pool de clientes por addr+db
)

// GetRedisClient obtiene un cliente Redis reutilizable.
// Si ya existe para ese addr/db → lo devuelve.
// Si no existe → crea uno nuevo, lo guarda y lo retorna.
func GetRedisClient(addr string, db int) *redis.Client {
	mu.Lock()
	defer mu.Unlock()

	key := fmt.Sprintf("%s:%d", addr, db)
	if c, ok := clients[key]; ok {
		return c
	}

	options := &redis.Options{
		Addr:         addr,
		DB:           db,
		PoolSize:     200,
		MinIdleConns: 20,
		PoolTimeout:  5 * time.Second,
		IdleTimeout:  5 * time.Minute,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		DialTimeout:  3 * time.Second,
	}
	if password := os.Getenv("CREDENTIALREDIS"); password != "" {
		options.Password = password
	}
	c := redis.NewClient(options)

	// Test inicial
	if err := c.Ping(ctx).Err(); err != nil {
		panic("failed to connect to Redis: " + err.Error())
	}

	clients[key] = c
	fmt.Println("Connected to Redis at", addr)
	return c
}

// Cierra todos los clientes Redis al apagar la app.
func CloseAll() {
	mu.Lock()
	defer mu.Unlock()
	for key, c := range clients {
		_ = c.Close()
		delete(clients, key)
	}
}

// Helpers ----------------------

func SetCache(key string, data any, ttl time.Duration) error {
	client := GetRedisClient(os.Getenv("URL_REDIS_SYSTEM"), 3)
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return client.Set(ctx, key, payload, ttl).Err()
}

func GetCache(key string, dest any) (bool, error) {
	client := GetRedisClient(os.Getenv("URL_REDIS_SYSTEM"), 3)
	val, err := client.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil // cache miss
	} else if err != nil {
		return false, err
	}
	return true, json.Unmarshal([]byte(val), dest)
}

func BuildRedisKey(prefix string, userID string, query map[string]interface{}) string {
	serialized, _ := json.Marshal(query)
	raw := prefix + ":" + userID + ":" + string(serialized)

	hasher := sha1.New()
	hasher.Write([]byte(raw))
	return prefix + ":" + hex.EncodeToString(hasher.Sum(nil))
}

func DeleteCacheByPrefix(prefix string) error {
	fmt.Println("Deleting cache by prefix:", prefix)
	client := GetRedisClient(os.Getenv("URL_REDIS_SYSTEM"), 3)

	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return err
		}

		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
