package analytics

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var Client *ClickHouseClient

type Config struct {
	URL      string
	Database string
	Table    string
	User     string
	Password string
	Timeout  time.Duration
}

type ClickHouseClient struct {
	baseURL  string
	database string
	table    string
	user     string
	password string
	client   *http.Client
}

func Connect(cfg Config) error {
	if strings.TrimSpace(cfg.URL) == "" {
		Client = nil
		return nil
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	clickhouseClient := &ClickHouseClient{
		baseURL:  strings.TrimSpace(cfg.URL),
		database: strings.TrimSpace(cfg.Database),
		table:    strings.TrimSpace(cfg.Table),
		user:     strings.TrimSpace(cfg.User),
		password: cfg.Password,
		client: &http.Client{
			Timeout: timeout,
		},
	}
	if clickhouseClient.table == "" {
		clickhouseClient.table = "solana_events"
	}

	if err := clickhouseClient.Ping(context.Background()); err != nil {
		return err
	}

	Client = clickhouseClient
	return nil
}

func Ping(ctx context.Context) error {
	if Client == nil {
		return errors.New("clickhouse client is not initialized")
	}

	return Client.Ping(ctx)
}

func QueryRows(ctx context.Context, query string) ([]map[string]any, error) {
	if Client == nil {
		return nil, errors.New("clickhouse client is not initialized")
	}

	return Client.QueryRows(ctx, query)
}

func EventsTable() (string, error) {
	if Client == nil {
		return "", errors.New("clickhouse client is not initialized")
	}
	return Client.table, nil
}

func (c *ClickHouseClient) Ping(ctx context.Context) error {
	body, err := c.doQuery(ctx, "SELECT 1 FORMAT TabSeparated")
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(body)) != "1" {
		return fmt.Errorf("unexpected clickhouse ping response: %s", strings.TrimSpace(string(body)))
	}

	return nil
}

func (c *ClickHouseClient) QueryRows(ctx context.Context, query string) ([]map[string]any, error) {
	body, err := c.doQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 1024*64), 1024*1024*10)

	rows := make([]map[string]any, 0, 64)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		row := make(map[string]any)
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("failed to decode clickhouse row: %w", err)
		}
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *ClickHouseClient) doQuery(ctx context.Context, query string) ([]byte, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("query cannot be empty")
	}

	parsedURL, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}

	queryValues := parsedURL.Query()
	if c.database != "" {
		queryValues.Set("database", c.database)
	}
	parsedURL.RawQuery = queryValues.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsedURL.String(), strings.NewReader(query))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "text/plain")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("clickhouse query failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}

	return payload, nil
}
