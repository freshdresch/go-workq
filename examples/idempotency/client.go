package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/freshdresch/go-data-structures/workq/examples/internal/inventory"
)

const maxApplyAttempts = 3

type adjustmentClient struct {
	address string
	http    *http.Client
}

func newAdjustmentClient(address string) (*adjustmentClient, error) {
	endpoint, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("parse destination address: %w", err)
	}
	if endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errors.New("destination must be an HTTP origin")
	}
	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		MaxConnsPerHost:        maxActiveRequests,
		MaxIdleConnsPerHost:    maxActiveRequests,
		IdleConnTimeout:        operationTimeout,
		ResponseHeaderTimeout:  2 * time.Second,
		MaxResponseHeaderBytes: maxResponseBytes,
	}
	return &adjustmentClient{
		address: (&url.URL{Scheme: endpoint.Scheme, Host: endpoint.Host}).String(),
		http: &http.Client{
			Transport: transport, Timeout: operationTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// The owner joins users before closing idle connections.
func (c *adjustmentClient) closeIdleConnections() {
	c.http.CloseIdleConnections()
}

// apply returns success only for a matching durable receipt. Unknown replies never
// authorize an Ack. Every retry preserves the original key, codec, and exact bytes.
// Duplicate reports GET recovery only. A successful POST can also return a saved
// receipt, so Duplicate=false does not establish a newly committed mutation.
func (c *adjustmentClient) apply(
	ctx context.Context,
	request inventory.Request,
) (inventory.Result, error) {
	key, err := request.Key.Encode()
	if err != nil {
		return inventory.Result{}, err
	}
	if len(request.Codec) == 0 || len(request.Codec) > inventory.MaxCodec ||
		len(request.Payload) == 0 || len(request.Payload) > inventory.MaxPayload {
		return inventory.Result{}, inventory.ErrRejected
	}
	var lastErr error
	for range maxApplyAttempts {
		if err := ctx.Err(); err != nil {
			return inventory.Result{}, fmt.Errorf("%w: %w", inventory.ErrUnresolved, err)
		}
		result, err := c.exchange(ctx, http.MethodPost, key, request)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, inventory.ErrUnresolved) {
			return inventory.Result{}, err
		}
		if ctx.Err() != nil {
			return inventory.Result{}, err
		}
		result, lastErr = c.exchange(ctx, http.MethodGet, key, request)
		if lastErr == nil {
			result.Duplicate = true
			return result, nil
		}
	}
	return inventory.Result{}, fmt.Errorf("destination attempts exhausted: %w", lastErr)
}

func (c *adjustmentClient) exchange(
	ctx context.Context,
	method string,
	key string,
	request inventory.Request,
) (inventory.Result, error) {
	address := c.address + "/adjustments"
	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewReader(request.Payload)
	} else {
		address += "/" + key
	}
	req, err := http.NewRequestWithContext(ctx, method, address, body)
	if err != nil {
		return inventory.Result{}, fmt.Errorf("%w: %w", inventory.ErrUnresolved, err)
	}
	if method == http.MethodPost {
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("X-Adjustment-Codec", request.Codec)
		req.Header.Set("Content-Type", "application/json")
		// Keep transport retries out of the explicit POST attempt budget.
		req.GetBody = nil
	}
	response, err := c.http.Do(req)
	if err != nil {
		return inventory.Result{}, fmt.Errorf("%w: %w", inventory.ErrUnresolved, err)
	}
	defer func() {
		// The body is read-only. Closing it cannot change the persisted receipt.
		_ = response.Body.Close()
	}()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return inventory.Result{}, fmt.Errorf("read receipt: %w: %w", inventory.ErrUnresolved, err)
	}
	if len(payload) > maxResponseBytes {
		return inventory.Result{}, fmt.Errorf("receipt too large: %w", inventory.ErrUnresolved)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return inventory.Result{}, fmt.Errorf("receipt content type: %w", inventory.ErrUnresolved)
	}
	if response.StatusCode == http.StatusOK {
		return matchingResult(payload, key, request)
	}
	return inventory.Result{}, responseError(method, response.StatusCode, payload)
}

func matchingResult(
	body []byte,
	key string,
	request inventory.Request,
) (inventory.Result, error) {
	receipt, err := decodeReceiptResponse(body)
	if err != nil {
		return inventory.Result{}, fmt.Errorf("decode receipt: %w: %w", inventory.ErrUnresolved, err)
	}
	if receipt.State != "succeeded" || receipt.Key != key || receipt.Codec != request.Codec ||
		!bytes.Equal(receipt.Request, request.Payload) {
		return inventory.Result{}, fmt.Errorf("receipt binding mismatch: %w", inventory.ErrUnresolved)
	}
	if receipt.Result.AdjustmentID != key || receipt.Result.Quantity < 0 {
		return inventory.Result{}, fmt.Errorf("invalid receipt result: %w", inventory.ErrUnresolved)
	}
	return inventory.Result{
		AdjustmentID: receipt.Result.AdjustmentID, Quantity: receipt.Result.Quantity,
	}, nil
}

func responseError(method string, status int, body []byte) error {
	envelope, err := decodeStateResponse(body)
	if err != nil {
		return fmt.Errorf("decode destination state: %w: %w", inventory.ErrUnresolved, err)
	}
	if method == http.MethodPost {
		switch {
		case status == http.StatusConflict && envelope.State == "conflict":
			return inventory.ErrConflict
		case status == http.StatusUnprocessableEntity && envelope.State == "rejected":
			return inventory.ErrRejected
		case status == http.StatusInsufficientStorage && envelope.State == "capacity":
			return inventory.ErrCapacity
		case (status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge) &&
			envelope.State == "malformed":
			return inventory.ErrRejected
		}
	}
	// Even an authoritative GET absence cannot fence an older in-flight POST.
	return fmt.Errorf("destination HTTP %d: %w", status, inventory.ErrUnresolved)
}
