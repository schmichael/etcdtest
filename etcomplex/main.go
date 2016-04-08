package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/schmichael/etcdtest/loggerapi"
	"golang.org/x/net/context"
)

func main() {
	url := "http://localhost:2379"
	flag.StringVar(&url, "etcd", url, "url of etcd")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer cancel()
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
		s := <-sigc
		log.Printf("Exiting due to signal: %v", s)
		return
	}()

	if err := Main(ctx, url); err != nil {
		log.Fatal(err)
	}
	log.Printf("done")
}

func Main(ctx context.Context, url string) error {
	conf := client.Config{Endpoints: []string{url}}

	// random unrelated background operations
	for i := 0; i < 10; i++ {
		go randomops(ctx, i, conf)
	}

	c, err := client.New(conf)
	if err != nil {
		panic(err)
	}
	cli := loggerapi.New(client.NewKeysAPI(c))

	log.Printf("--Create a dir without a ttl")
	if _, err := cli.Set(ctx, "/schmichael", "", &client.SetOptions{Dir: true}); err != nil {
		return err
	}

	log.Printf("--Create a key within the dir with a ttl")
	ttl := 5 * time.Second
	if _, err := cli.Set(ctx, "/schmichael/owner", "foo", &client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl}); err != nil {
		return err
	}

	log.Printf("--Wait a second and then refresh the key")
	time.Sleep(time.Second)
	refreshopts := &client.SetOptions{Refresh: true, PrevExist: client.PrevExist, TTL: ttl, PrevValue: "foo"}
	if _, err := cli.Set(ctx, "/schmichael/owner", "foo", refreshopts); err != nil {
		return err
	}

	log.Printf("--Wait a second and then compare-and-delete the key")
	time.Sleep(time.Second)
	delopts := &client.DeleteOptions{PrevValue: "foo"}
	delresp, err := cli.Delete(ctx, "/schmichael/owner", delopts)
	if err != nil {
		return err
	}
	delindex := delresp.Index

	log.Printf("--Wait a second and then watch /schmichael/ with AfterIndex=%d", delindex)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	wwg := sync.WaitGroup{}
	wwg.Add(1)
	go func() {
		defer wwg.Done()
		time.Sleep(time.Second)
		w := cli.Watcher("/schmichael/", &client.WatcherOptions{AfterIndex: delindex, Recursive: true})
		for resp, err := w.Next(wctx); err == nil; {
			if resp.Node == nil {
				log.Printf("Response with no node?!")
				continue
			}
			if resp.Node.ModifiedIndex < delindex {
				log.Printf("Response with modified index < afterindex? %d < %d", resp.Node.ModifiedIndex, delindex)
			}
		}
		if err != context.Canceled {
			log.Printf("ERROR watch loop exited with error: %v", err)
		}
		log.Printf("Watch loop exited cleanly")
	}()

	log.Printf("--Wait a second and then create /schmichael/foo with a ttl")
	time.Sleep(time.Second)
	if _, err := cli.Set(ctx, "/schmichael/owner", "foo", &client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl}); err != nil {
		return err
	}

	log.Printf("--Wait 5 seconds to let it expire and then exit")
	time.Sleep(5100 * time.Millisecond)
	wcancel()
	wwg.Wait()
	return nil
}

// randomops does random operations against etcd
func randomops(ctx context.Context, id int, conf client.Config) {
	c, err := client.New(conf)
	if err != nil {
		panic(err)
	}

	cli := client.NewKeysAPI(c)

	url := fmt.Sprintf("/schmichael-%d/test", id)
	var i int64
	for ; ; i++ {
		time.Sleep((time.Duration(id) * time.Millisecond) + time.Millisecond)
		switch i % 2 {
		case 0:
			_, err := cli.Set(ctx, url, strconv.FormatInt(i, 10), nil)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Printf("randomops(%d) ERROR i=%d %v", id, i, err)
				continue
			}
		default:
			_, err := cli.Delete(ctx, url, nil)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Printf("randomops(%d) ERROR i=%d %v", id, i, err)
				continue
			}
		}

		if i > 0 && i%100 == 0 {
			log.Printf("randomops(%d) -> %d", id, i)
		}
	}
}
