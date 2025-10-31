package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/knx-go/knx-go/knx"
	"github.com/knx-go/knx-go/knx/cemi"
	"github.com/knx-go/knx-go/knx/dpt"
	"github.com/knx-go/knx-go/knx/gac"
	"github.com/knx-go/knx-go/knx/util"
)

// Options controls the behaviour of the KNX proxy server.
type Options struct {
	// GatewayAddress identifies the KNXnet/IP gateway to connect to.
	GatewayAddress string
	// ListenAddress defines the HTTP listen address. Defaults to ":8080".
	ListenAddress string
	// EventLimit caps the number of events retained in memory.
	EventLimit int
	// LogEvents toggles logging of inbound KNX group traffic.
	LogEvents bool
	// Catalog optionally provides group metadata for name lookups.
	Catalog *gac.Catalog
	// ResponseTimeout bounds how long the proxy will wait for responses.
	ResponseTimeout time.Duration
	// Logger writes operational logs. When nil a standard stdout logger is used.
	Logger *log.Logger
	// Store optionally persists events for historical queries.
	Store EventStore
}

// EventStore allows the proxy to persist and retrieve KNX events.
type EventStore interface {
	Record(ctx context.Context, event Event) error
	Latest(ctx context.Context, destination string) (Event, bool, error)
	History(ctx context.Context, destination string, limit int) ([]Event, error)
}

type server struct {
	client     *knx.GroupTunnel
	logger     *log.Logger
	logEvents  bool
	eventLimit int

	eventsMu        sync.RWMutex
	events          []Event
	catalog         *gac.Catalog
	responseTimeout time.Duration
	store           EventStore

	pendingMu sync.Mutex
	pending   map[string][]chan Event
}

// Event describes a KNX group event observed by the proxy.
type Event struct {
	Timestamp   time.Time `json:"timestamp"`
	Command     string    `json:"command"`
	Source      string    `json:"source"`
	Destination string    `json:"destination"`
	Data        []byte    `json:"data,omitempty"`
	Group       string    `json:"group,omitempty"`
	DPT         string    `json:"dpt,omitempty"`
	Decoded     string    `json:"decoded,omitempty"`
}

type groupRequest struct {
	Command         string `json:"command"`
	Destination     string `json:"destination"`
	Source          string `json:"source,omitempty"`
	Data            []byte `json:"data,omitempty"`
	WaitForResponse bool   `json:"wait_for_response,omitempty"`
	Timeout         string `json:"timeout,omitempty"`
}

type groupResponse struct {
	Status string `json:"status"`
	Event  *Event `json:"event,omitempty"`
}

type groupInfo struct {
	Name    string   `json:"name"`
	Address string   `json:"address"`
	Path    []string `json:"path,omitempty"`
	DPTs    []string `json:"dpts,omitempty"`
}

// Run launches the KNX proxy server using the supplied options until the
// provided context is cancelled or a fatal error occurs.
func Run(ctx context.Context, opts Options) error {
	if opts.GatewayAddress == "" {
		return errors.New("gateway address is required")
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	util.Logger = logger

	listenAddr := opts.ListenAddress
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	tunnel, err := knx.NewGroupTunnel(opts.GatewayAddress, knx.DefaultTunnelConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to gateway %s: %w", opts.GatewayAddress, err)
	}
	defer tunnel.Close()

	srv := newServer(&tunnel, logger, opts.EventLimit, opts.LogEvents, opts.Catalog, opts.ResponseTimeout, opts.Store)
	srv.start(ctx)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: srv.routes(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("knxproxy: HTTP server shutdown error: %v", err)
		}
	}()

	logger.Printf("knxproxy: connected to KNX gateway %s", opts.GatewayAddress)
	logger.Printf("knxproxy: listening on %s", listenAddr)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("knxproxy: HTTP server error: %w", err)
	}

	logger.Println("knxproxy: shutdown complete")
	return nil
}

func newServer(client *knx.GroupTunnel, logger *log.Logger, eventLimit int, logEvents bool, catalog *gac.Catalog, responseTimeout time.Duration, store EventStore) *server {
	if eventLimit <= 0 {
		eventLimit = 64
	}

	return &server{
		client:     client,
		logger:     logger,
		logEvents:  logEvents,
		eventLimit: eventLimit,
		events:     make([]Event, 0, eventLimit),
		catalog:    catalog,
		store:      store,
		pending:    make(map[string][]chan Event),
		responseTimeout: func() time.Duration {
			if responseTimeout <= 0 {
				return 5 * time.Second
			}
			return responseTimeout
		}(),
	}
}

func (s *server) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, open := <-s.client.Inbound():
				if !open {
					s.logger.Println("knxproxy: KNX inbound channel closed")
					return
				}

				event := Event{
					Timestamp:   time.Now().UTC(),
					Command:     evt.Command.String(),
					Source:      evt.Source.String(),
					Destination: s.formatAddress(evt.Destination),
				}

				if len(evt.Data) > 0 {
					event.Data = append([]byte(nil), evt.Data...)
				}

				s.applyCatalogMetadata(&event, evt.Destination, evt.Data)

				s.storeEvent(event)

				if s.logEvents {
					s.logger.Printf("knxproxy: received %s from %s to %s", event.Command, event.Source, event.Destination)
				}

				s.notifyWaiters(event)
			}
		}
	}()
}

func (s *server) storeEvent(event Event) {
	s.eventsMu.Lock()
	if len(s.events) == s.eventLimit {
		copy(s.events, s.events[1:])
		s.events = s.events[:s.eventLimit-1]
	}
	s.events = append(s.events, event)
	s.eventsMu.Unlock()

	if s.store != nil {
		go s.persistEvent(event)
	}
}

func (s *server) persistEvent(event Event) {
	if s.store == nil {
		return
	}
	if err := s.store.Record(context.Background(), event); err != nil && s.logger != nil {
		s.logger.Printf("knxproxy: failed to persist event: %v", err)
	}
}

func (s *server) snapshotEvents() []Event {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()

	events := make([]Event, len(s.events))
	copy(events, s.events)
	return events
}

func (s *server) notifyWaiters(event Event) {
	key := event.Destination

	s.pendingMu.Lock()
	waiters := s.pending[key]
	if len(waiters) > 0 {
		delete(s.pending, key)
	}
	s.pendingMu.Unlock()

	for _, ch := range waiters {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *server) removeWaiter(key string, ch chan Event) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	waiters := s.pending[key]
	for i, w := range waiters {
		if w == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}

	if len(waiters) == 0 {
		delete(s.pending, key)
	} else {
		s.pending[key] = waiters
	}
}

func (s *server) awaitResponse(ctx context.Context, dest cemi.GroupAddr, timeout time.Duration) (Event, error) {
	if timeout <= 0 {
		timeout = s.responseTimeout
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	ch := make(chan Event, 1)
	key := s.formatAddress(dest)

	s.pendingMu.Lock()
	s.pending[key] = append(s.pending[key], ch)
	s.pendingMu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		s.removeWaiter(key, ch)
		return Event{}, ctx.Err()
	case <-timer.C:
		s.removeWaiter(key, ch)
		return Event{}, context.DeadlineExceeded
	case event := <-ch:
		return event, nil
	}
}

func (s *server) groupNameForAddress(addr cemi.GroupAddr) string {
	if s.catalog != nil {
		if group, ok := s.catalog.LookupByAddress(addr); ok {
			return group.Name
		}
	}
	return ""
}

func (s *server) formatAddress(addr cemi.GroupAddr) string {
	if s.catalog != nil {
		return s.catalog.FormatAddress(addr)
	}
	return addr.String()
}

func describeDatapoints(group *gac.Group, data []byte) string {
	if group == nil || len(group.DPTs) == 0 || len(data) == 0 {
		return ""
	}

	parts := make([]string, 0, len(group.DPTs))
	for _, dp := range group.DPTs {
		value, ok := dp.Produce()
		if !ok {
			continue
		}

		if err := value.Unpack(data); err != nil {
			parts = append(parts, fmt.Sprintf("%s=<decode error: %v>", string(dp), err))
			continue
		}

		rendered := fmt.Sprintf("%s=%s", string(dp), fmt.Sprint(value))
		if meta, ok := value.(dpt.DatapointMeta); ok {
			if unit := meta.Unit(); unit != "" {
				rendered = fmt.Sprintf("%s %s", rendered, unit)
			}
		}

		parts = append(parts, rendered)
	}

	return strings.Join(parts, ", ")
}

func (s *server) parseDestination(value string) (cemi.GroupAddr, string, bool, error) {
	destination := strings.TrimSpace(value)
	if destination == "" {
		return 0, "", false, errors.New("destination is required")
	}

	if s.catalog != nil {
		if group, ok := s.catalog.Lookup(destination); ok {
			return group.Address, group.Name, true, nil
		}
	}

	addr, err := cemi.NewGroupAddrString(destination)
	if err != nil {
		return 0, "", looksLikeName(destination), fmt.Errorf("destination must be a known group name or a valid group address: %w", err)
	}

	return addr, s.groupNameForAddress(addr), false, nil
}

func looksLikeName(value string) bool {
	var hasLetter bool
	for _, r := range value {
		if r == '/' || r == '.' {
			return false
		}
		if unicode.IsLetter(r) {
			hasLetter = true
		}
	}
	return hasLetter
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/openapi.json", s.handleOpenAPI)
	mux.HandleFunc("/v1/group", s.handleGroup)
	mux.HandleFunc("/v1/group/", s.handleGroupRead)
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/v1/groups", s.handleGroupList)
	return mux
}

func (s *server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(OpenAPISpec()); err != nil {
		s.logger.Printf("knxproxy: failed to write OpenAPI spec: %v", err)
	}
}

func (s *server) handleGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req groupRequest
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	cmd, err := parseCommand(req.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dest, groupName, lookedLikeName, err := s.parseDestination(req.Destination)
	if err != nil {
		status := http.StatusBadRequest
		if lookedLikeName {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	var (
		src        cemi.PhysicalAddr
		sourceText string
	)
	if req.Source != "" {
		src, err = cemi.NewPhysicalAddrString(req.Source)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid source: %v", err), http.StatusBadRequest)
			return
		}
		sourceText = src.String()
	}

	if cmd != knx.GroupRead && len(req.Data) == 0 {
		http.Error(w, "data is required for this command", http.StatusBadRequest)
		return
	}

	event := knx.GroupEvent{
		Command:     cmd,
		Destination: dest,
		Data:        req.Data,
	}

	if req.Source != "" {
		event.Source = src
	}

	if err := s.client.Send(event); err != nil {
		http.Error(w, fmt.Sprintf("failed to send group event: %v", err), http.StatusBadGateway)
		return
	}

	sentEvent := Event{
		Timestamp:   time.Now().UTC(),
		Command:     cmd.String(),
		Source:      sourceText,
		Destination: s.formatAddress(dest),
	}
	if len(req.Data) > 0 {
		sentEvent.Data = append([]byte(nil), req.Data...)
	}

	s.applyCatalogMetadata(&sentEvent, dest, req.Data)
	if sentEvent.Group == "" {
		sentEvent.Group = groupName
	}

	s.storeEvent(sentEvent)
	if s.logEvents {
		s.logger.Printf("knxproxy: sent %s to %s", sentEvent.Command, sentEvent.Destination)
	}

	waitForResponse := req.WaitForResponse
	if cmd == knx.GroupRead {
		waitForResponse = true
	}

	timeout := s.responseTimeout
	if req.Timeout != "" {
		parsed, err := time.ParseDuration(req.Timeout)
		if err != nil || parsed <= 0 {
			http.Error(w, "timeout must be a positive duration", http.StatusBadRequest)
			return
		}
		timeout = parsed
	}

	response := groupResponse{Status: "ok"}
	if waitForResponse {
		received, err := s.awaitResponse(r.Context(), dest, timeout)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				http.Error(w, "timeout waiting for group response", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, fmt.Sprintf("failed to receive group response: %v", err), http.StatusBadGateway)
			return
		}
		response.Event = &received
	} else {
		response.Event = &sentEvent
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Printf("knxproxy: failed to encode response: %v", err)
	}
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	events := s.snapshotEvents()

	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil || limit < 0 {
			http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
			return
		}
		if limit < len(events) {
			events = events[len(events)-limit:]
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(events); err != nil {
		s.logger.Printf("knxproxy: failed to encode events: %v", err)
	}
}

func (s *server) handleGroupList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	infos := make([]groupInfo, 0)
	if s.catalog != nil {
		groups := s.catalog.Groups()
		infos = make([]groupInfo, 0, len(groups))
		for _, group := range groups {
			info := groupInfo{
				Name:    group.Name,
				Address: group.AddressString(),
			}
			if len(group.Path) > 0 {
				info.Path = append([]string(nil), group.Path...)
			}
			if len(group.DPTs) > 0 {
				info.DPTs = make([]string, len(group.DPTs))
				for i, dpt := range group.DPTs {
					info.DPTs[i] = string(dpt)
				}
			}
			infos = append(infos, info)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(infos); err != nil {
		s.logger.Printf("knxproxy: failed to encode groups: %v", err)
	}
}

func (s *server) handleGroupRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	remainder := strings.TrimPrefix(r.URL.Path, "/v1/group/")
	remainder = strings.Trim(remainder, "/")
	if remainder == "" {
		http.Error(w, "group identifier is required", http.StatusBadRequest)
		return
	}

	var action string
	identifier := remainder
	if idx := strings.Index(remainder, "/"); idx >= 0 {
		identifier = remainder[:idx]
		action = strings.Trim(remainder[idx+1:], "/")
	}

	decoded, err := url.PathUnescape(identifier)
	if err != nil {
		http.Error(w, "invalid group identifier", http.StatusBadRequest)
		return
	}

	dest, groupName, lookedLikeName, err := s.parseDestination(decoded)
	if err != nil {
		status := http.StatusBadRequest
		if lookedLikeName {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	if action != "" {
		switch action {
		case "latest":
			s.handleGroupLatest(w, r, dest, groupName)
			return
		case "history":
			s.handleGroupHistory(w, r, dest, groupName)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}

	timeout := s.responseTimeout
	if tParam := r.URL.Query().Get("timeout"); tParam != "" {
		parsed, err := time.ParseDuration(tParam)
		if err != nil || parsed <= 0 {
			http.Error(w, "timeout must be a positive duration", http.StatusBadRequest)
			return
		}
		timeout = parsed
	}

	if err := s.client.Send(knx.GroupEvent{Command: knx.GroupRead, Destination: dest}); err != nil {
		http.Error(w, fmt.Sprintf("failed to send group read: %v", err), http.StatusBadGateway)
		return
	}

	sentEvent := Event{
		Timestamp:   time.Now().UTC(),
		Command:     knx.GroupRead.String(),
		Destination: s.formatAddress(dest),
	}
	s.applyCatalogMetadata(&sentEvent, dest, nil)
	if sentEvent.Group == "" {
		sentEvent.Group = groupName
	}
	s.storeEvent(sentEvent)
	if s.logEvents {
		s.logger.Printf("knxproxy: sent %s to %s", sentEvent.Command, sentEvent.Destination)
	}

	received, err := s.awaitResponse(r.Context(), dest, timeout)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "timeout waiting for group response", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, fmt.Sprintf("failed to receive group response: %v", err), http.StatusBadGateway)
		return
	}

	response := groupResponse{Status: "ok", Event: &received}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Printf("knxproxy: failed to encode response: %v", err)
	}
}

func (s *server) handleGroupLatest(w http.ResponseWriter, r *http.Request, dest cemi.GroupAddr, groupName string) {
	if s.store == nil {
		http.Error(w, "event history is not available", http.StatusNotFound)
		return
	}

	event, ok, err := s.store.Latest(r.Context(), s.formatAddress(dest))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to load latest event: %v", err), http.StatusBadGateway)
		return
	}
	if !ok {
		http.Error(w, "no events recorded for this group address", http.StatusNotFound)
		return
	}

	s.finalizeStoredEvent(&event, dest, groupName)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(event); err != nil {
		s.logger.Printf("knxproxy: failed to encode latest event: %v", err)
	}
}

func (s *server) handleGroupHistory(w http.ResponseWriter, r *http.Request, dest cemi.GroupAddr, groupName string) {
	if s.store == nil {
		http.Error(w, "event history is not available", http.StatusNotFound)
		return
	}

	limit := 0
	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		parsed, err := strconv.Atoi(limitParam)
		if err != nil || parsed < 0 {
			http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
			return
		}
		limit = parsed
	}

	events, err := s.store.History(r.Context(), s.formatAddress(dest), limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to load history: %v", err), http.StatusBadGateway)
		return
	}

	for i := range events {
		s.finalizeStoredEvent(&events[i], dest, groupName)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(events); err != nil {
		s.logger.Printf("knxproxy: failed to encode history: %v", err)
	}
}

func (s *server) finalizeStoredEvent(event *Event, addr cemi.GroupAddr, fallbackName string) {
	s.applyCatalogMetadata(event, addr, event.Data)
	if event.Group == "" {
		event.Group = fallbackName
	}
}

func (s *server) applyCatalogMetadata(event *Event, addr cemi.GroupAddr, data []byte) {
	event.Destination = s.formatAddress(addr)
	if s.catalog == nil {
		return
	}

	group, ok := s.catalog.LookupByAddress(addr)
	if !ok {
		return
	}

	event.Group = group.Name
	if event.DPT == "" && len(group.DPTs) > 0 {
		event.DPT = string(group.DPTs[0])
	}
	if len(data) == 0 {
		return
	}
	if event.Decoded != "" {
		return
	}

	if rendered := describeDatapoints(group, data); rendered != "" {
		event.Decoded = rendered
	}
}

func parseCommand(value string) (knx.GroupCommand, error) {
	if value == "" {
		return knx.GroupWrite, nil
	}

	switch strings.ToLower(value) {
	case "write":
		return knx.GroupWrite, nil
	case "read":
		return knx.GroupRead, nil
	case "response":
		return knx.GroupResponse, nil
	default:
		return 0, fmt.Errorf("unknown command %q", value)
	}
}
