// Package flussonic is a minimal client for the Flussonic Media Server HTTP API,
// covering only what the event pipeline needs: getStream (bandwidth/stats) and
// getSession (viewer IP/country enrichment).
//
// Auth mirrors backendV2: Authorization: Basic base64(FLUSSONIC_API_TOKEN).
package flussonic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type StreamStats struct {
	InputBitrate  float64 `json:"input_bitrate"`
	Bitrate       float64 `json:"bitrate"`
	BytesOut      int64   `json:"bytes_out"`
	OnlineClients int     `json:"online_clients"`
	Status        string  `json:"status"`
	Play          struct {
		PlayBytes int64 `json:"play_bytes"`
	} `json:"play"`
}

type StreamInfo struct {
	Title string       `json:"title"`
	Stats *StreamStats `json:"stats"`
}

type Session struct {
	IP      string `json:"ip"`
	Country string `json:"country"`
}

type Client struct {
	baseURL    string
	authHeader string
	http       *http.Client
	sem        chan struct{} // caps concurrent in-flight requests
}

func New(baseURL, token string, timeout time.Duration, maxInFlight int) *Client {
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	return &Client{
		baseURL:    baseURL,
		authHeader: "Basic " + base64.StdEncoding.EncodeToString([]byte(token)),
		http:       &http.Client{Timeout: timeout},
		sem:        make(chan struct{}, maxInFlight),
	}
}

// GetStream → GET /streamer/api/v3/streams/{name}
func (c *Client) GetStream(ctx context.Context, name string) (*StreamInfo, error) {
	var out StreamInfo
	if err := c.get(ctx, "/streamer/api/v3/streams/"+name, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSession → GET /streamer/api/v3/sessions/{id}. Returns (nil, nil) on 404.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	var out Session
	err := c.get(ctx, "/streamer/api/v3/sessions/"+url.PathEscape(sessionID), &out)
	if err != nil {
		if err == errNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

var errNotFound = fmt.Errorf("flussonic: 404 not found")

func (c *Client) get(ctx context.Context, path string, dst any) error {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.authHeader)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("flussonic: %s -> %d: %s", path, resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}
