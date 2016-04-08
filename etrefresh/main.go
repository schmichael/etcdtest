package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
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

	//XXX If you set a value here you get an error
	//    If you don't set a value here it clears the key's value
	if _, err := cli.Set(ctx, "/schmichael/owner", "", refreshopts); err != nil {
		return err
	}

	log.Printf("--Wait a second and then compare-and-delete the key")
	time.Sleep(time.Second)
	delopts := &client.DeleteOptions{PrevValue: "foo"}
	delresp, err := cli.Delete(ctx, "/schmichael/owner", delopts)
	if err != nil {
		return err
	}

	log.Printf("It worked!")
	return nil
}
