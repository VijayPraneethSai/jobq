// Package client is a small typed HTTP client for the coordinator API, used by the worker, jobqctl and loadgen.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/queue"
)

// ErrLeaseLost means the coordinator no longer considers the caller the owner of the job (HTTP 409).
var ErrLeaseLost = errors.New("lease lost")

// ErrNotFound means the job id is unknown (HTTP 404).
var ErrNotFound = errors.New("job not found")

// Client talks to one coordinator.
type Client struct {
	Base  string // e.g. http://127.0.0.1:8080
	Token string // bearer token, optional
	HTTP  *http.Client
}

func New(base, token string) *Client {
	return &Client{Base: base, Token: token, HTTP: &http.Client{Timeout: 15 * time.Second}}
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return 0, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusConflict:
		return resp.StatusCode, ErrLeaseLost
	case resp.StatusCode == http.StatusNotFound:
		return resp.StatusCode, ErrNotFound
	case resp.StatusCode >= 400:
		var e struct{ Error string }
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&e)
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, e.Error)
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

// SubmitReq mirrors the POST /jobs body.
type SubmitReq struct {
	Key        string   `json:"key,omitempty"`
	Command    string   `json:"command"`
	Args       []string `json:"args,omitempty"`
	TimeoutSec int      `json:"timeout_sec,omitempty"`
	MaxRetries int      `json:"max_retries,omitempty"`
	Priority   int      `json:"priority,omitempty"`
}

// Submit queues a job. created is false when the idempotency key already existed.
func (c *Client) Submit(ctx context.Context, r SubmitReq) (j queue.Job, created bool, err error) {
	code, err := c.do(ctx, http.MethodPost, "/jobs", r, &j)
	return j, code == http.StatusCreated, err
}

func (c *Client) Get(ctx context.Context, id string) (j queue.Job, err error) {
	_, err = c.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id), nil, &j)
	return j, err
}

func (c *Client) List(ctx context.Context, state string, limit int) (js []queue.Job, err error) {
	q := url.Values{}
	if state != "" {
		q.Set("state", state)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprint(limit))
	}
	_, err = c.do(ctx, http.MethodGet, "/jobs?"+q.Encode(), nil, &js)
	return js, err
}

// Lease asks for the next job; (nil, nil) means the queue is empty right now.
func (c *Client) Lease(ctx context.Context, worker string, ttl time.Duration) (*queue.Job, error) {
	var j queue.Job
	code, err := c.do(ctx, http.MethodPost, "/lease", map[string]any{"worker": worker, "ttl_sec": int(ttl.Seconds())}, &j)
	if err != nil || code == http.StatusNoContent {
		return nil, err
	}
	return &j, nil
}

func (c *Client) Heartbeat(ctx context.Context, id, token string, ttl time.Duration) error {
	_, err := c.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/heartbeat", map[string]any{"token": token, "ttl_sec": int(ttl.Seconds())}, nil)
	return err
}

// Report is the result of one attempt.
type Report struct {
	Token      string `json:"token"`
	ExitCode   int    `json:"exit_code"`
	Output     string `json:"output"`
	DurationMS int64  `json:"duration_ms"`
	TimedOut   bool   `json:"timed_out"`
}

func (c *Client) Complete(ctx context.Context, id string, r Report) (j queue.Job, err error) {
	_, err = c.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/complete", r, &j)
	return j, err
}
