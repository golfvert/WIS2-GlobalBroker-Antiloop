// Package redisconn parses REDIS_URL and builds a client.
//
// REDIS_URL is not a single "redis://host:port" string — it's a JSON
// array of {host, port} objects, matching the original flow's
// redis-config node: `"cluster": true, "optionsType": "jsonata",
// "options": "$eval($env(\"REDIS_URL\"))"`. This package is the single
// shared constructor for that format, used by every component that
// needs a Redis connection.
package redisconn

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

type node struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// New parses the REDIS_URL JSON-array format and returns either a
// cluster client or a single-instance client, per cluster.
//
// cluster=false builds a plain *redis.Client, and requires REDIS_URL to
// contain exactly one {host,port} entry — anything else is a
// configuration error and fails startup outright, rather than silently
// using the first entry and discarding the rest. Return type is
// redis.UniversalClient rather than a concrete type
// specifically so callers (Close(), Ping(), and every internal/*
// package that already accepts the narrower redis.Cmdable —
// UniversalClient satisfies that too) don't need to know or care which
// mode is active.
//
// minIdleConns is set explicitly rather than left at go-redis's default
// of 0 (lazy, on-first-use connection creation per goroutine), so that
// the first burst of concurrent messages after startup doesn't pay real
// TCP-handshake-plus-cluster-negotiation cost on top of the actual SET
// NX round trip — a cost that would otherwise be indistinguishable, in
// the "dedup" timing category, from genuine Redis latency. WarmUp
// (below) complements this by forcing connections into existence
// immediately at startup rather than leaving even that initial
// creation to happen lazily on real traffic.
//
// Callers should size minIdleConns against cfg.DedupBatchConcurrency:
// internal/dedup.NewBatched is the one place in this codebase with a
// fixed, small cap on concurrent Redis round trips (pipelined Execs),
// so it's the number that actually describes how many connections are
// worth pre-warming — there's no general fixed worker-pool concurrency
// figure to size against instead, since relay.Pipeline gives every
// message its own goroutine.
func New(redisURLJSON string, minIdleConns int, cluster bool) (redis.UniversalClient, error) {
	var nodes []node
	if err := json.Unmarshal([]byte(redisURLJSON), &nodes); err != nil {
		return nil, fmt.Errorf("REDIS_URL is not the expected JSON array of {host,port}: %w", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("REDIS_URL parsed to zero nodes")
	}

	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = fmt.Sprintf("%s:%d", n.Host, n.Port)
	}

	if cluster {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			MinIdleConns: minIdleConns,
		}), nil
	}

	if len(addrs) != 1 {
		return nil, fmt.Errorf("REDIS_CLUSTER=false requires REDIS_URL to contain exactly one {host,port} entry, got %d", len(addrs))
	}
	return redis.NewClient(&redis.Options{
		Addr:         addrs[0],
		MinIdleConns: minIdleConns,
	}), nil
}

// WarmUp forces n connections into existence per cluster shard, right
// now, concurrently — rather than relying on MinIdleConns' background
// pool maintenance to get there on its own time (not guaranteed before
// the first real message arrives), and rather than a plain top-level
// rdb.Ping(ctx) repeated n times, which — since PING carries no key to
// hash — go-redis routes to whatever single node it picks by default,
// not spread across the cluster. dedup's keys (wis2gb:dedup:<id>) hash
// to essentially random slots across every shard, so what actually
// needs to be warm is a connection to EVERY shard, not n connections to
// one of them. ForEachShard runs fn once per shard's own *redis.Client,
// concurrently across shards; firing n concurrent Pings per shard
// (matching relay.Pipeline's worker count) forces that many real
// connections open to each one before any live traffic arrives.
//
// Call once at startup, after New and after the initial connectivity
// Ping. Best-effort throughout: logs failures but doesn't treat them as
// fatal — a shard/connection that fails to warm up just falls back to
// lazy on-demand connection creation later, same as before this
// existed.
//
// rdb is redis.UniversalClient since New can hand back either a
// *redis.ClusterClient or a *redis.Client. The per-shard ForEachShard
// fan-out only makes sense for the cluster case — a single instance has
// no shards, just N connections worth warming against the one node —
// so this type-switches and runs the simpler single-node loop when
// cluster mode is off.
func WarmUp(ctx context.Context, rdb redis.UniversalClient, n int) {
	if n < 1 {
		return
	}
	switch c := rdb.(type) {
	case *redis.ClusterClient:
		err := c.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := shard.Ping(ctx).Err(); err != nil {
						log.Printf("redis: warm-up connection to %s failed (will be created lazily on first real use instead): %v", shard.Options().Addr, err)
					}
				}()
			}
			wg.Wait()
			return nil
		})
		if err != nil {
			log.Printf("redis: warm-up failed to enumerate cluster shards: %v", err)
		}
	case *redis.Client:
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.Ping(ctx).Err(); err != nil {
					log.Printf("redis: warm-up connection to %s failed (will be created lazily on first real use instead): %v", c.Options().Addr, err)
				}
			}()
		}
		wg.Wait()
	default:
		log.Printf("redis: warm-up skipped — unrecognized client type %T", rdb)
	}
}
