// Command jobqctl is a small command-line client: submit, status, list, wait.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: jobqctl [-coordinator URL] [-token T] <command>
  submit [-key K] [-timeout S] [-retries N] [-priority P] -- <command> [args...]
  status <id>
  list [-state queued|running|succeeded|dead] [-limit N]
  wait <id> [-timeout 5m]    exits 0 if the job succeeded, 1 if it is dead, 2 on timeout`)
	os.Exit(2)
}

func main() {
	coord := flag.String("coordinator", "http://127.0.0.1:8080", "coordinator base URL")
	token := flag.String("token", os.Getenv("JOBQ_TOKEN"), "bearer token (default $JOBQ_TOKEN)")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	c := client.New(*coord, *token)
	ctx := context.Background()
	cmd, args := flag.Arg(0), flag.Args()[1:]
	show := func(v any) { b, _ := json.MarshalIndent(v, "", "  "); fmt.Println(string(b)) }
	fail := func(err error) { fmt.Fprintln(os.Stderr, "error:", err); os.Exit(1) }

	switch cmd {
	case "submit":
		fs := flag.NewFlagSet("submit", flag.ExitOnError)
		key := fs.String("key", "", "idempotency key")
		to := fs.Int("timeout", 0, "per-attempt timeout in seconds (0 = worker default)")
		retries := fs.Int("retries", 0, "retries after the first attempt")
		prio := fs.Int("priority", 0, "higher runs first")
		_ = fs.Parse(args)
		rest := fs.Args()
		if len(rest) == 0 {
			usage()
		}
		j, created, err := c.Submit(ctx, client.SubmitReq{Key: *key, Command: rest[0], Args: rest[1:], TimeoutSec: *to, MaxRetries: *retries, Priority: *prio})
		if err != nil {
			fail(err)
		}
		if !created {
			fmt.Fprintln(os.Stderr, "note: idempotency key already used; showing the existing job")
		}
		show(j)
	case "status":
		if len(args) != 1 {
			usage()
		}
		j, err := c.Get(ctx, args[0])
		if err != nil {
			fail(err)
		}
		show(j)
	case "list":
		fs := flag.NewFlagSet("list", flag.ExitOnError)
		state := fs.String("state", "", "filter by state")
		limit := fs.Int("limit", 50, "max jobs")
		_ = fs.Parse(args)
		js, err := c.List(ctx, *state, *limit)
		if err != nil {
			fail(err)
		}
		for _, j := range js {
			fmt.Printf("%-8s %-10s attempt=%d exit=%d %s %v\n", j.ID, j.State, j.Attempt, j.ExitCode, j.Command, j.Args)
		}
	case "wait":
		fs := flag.NewFlagSet("wait", flag.ExitOnError)
		to := fs.Duration("timeout", 5*time.Minute, "give up after")
		if len(args) < 1 {
			usage()
		}
		_ = fs.Parse(args[1:])
		deadline := time.Now().Add(*to)
		for time.Now().Before(deadline) {
			j, err := c.Get(ctx, args[0])
			if err != nil {
				fail(err)
			}
			switch j.State {
			case "succeeded":
				show(j)
				return
			case "dead":
				show(j)
				os.Exit(1)
			}
			time.Sleep(300 * time.Millisecond)
		}
		fmt.Fprintln(os.Stderr, "timed out waiting")
		os.Exit(2)
	default:
		usage()
	}
}
