package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/knx-go/knx-go/knx/gac"
	"github.com/knx-go/knx-go/knx/proxy"
)

func main() {
	var (
		gatewayAddr = flag.String("gateway", "", "KNXnet/IP gateway address (host:port)")
		listenAddr  = flag.String("listen", ":8080", "HTTP listen address")
		eventLimit  = flag.Int("event-buffer", 64, "maximum number of events to retain in memory")
		logEvents   = flag.Bool("log-events", false, "log inbound KNX group events")
		groupFile   = flag.String("group-file", "", "path to a KNX group address export (XML)")
		responseTO  = flag.Duration("response-timeout", 5*time.Second, "maximum time to wait for group responses (e.g. 5s)")
	)

	flag.Parse()

	if *gatewayAddr == "" {
		fmt.Fprintln(os.Stderr, "gateway address is required")
		flag.Usage()
		os.Exit(2)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var catalog *gac.Catalog
	if *groupFile != "" {
		file, err := os.Open(*groupFile)
		if err != nil {
			logger.Fatalf("knxproxy: failed to open group file %q: %v", *groupFile, err)
		}

		catalog, err = gac.Import(file)
		if closeErr := file.Close(); closeErr != nil {
			logger.Fatalf("knxproxy: failed to close group file %q: %v", *groupFile, closeErr)
		}
		if err != nil {
			logger.Fatalf("knxproxy: failed to parse group file %q: %v", *groupFile, err)
		}

		logger.Printf("knxproxy: loaded %d group addresses from %s", len(catalog.Groups()), *groupFile)
	}

	opts := proxy.Options{
		GatewayAddress: *gatewayAddr,
		ListenAddress:  *listenAddr,
		EventLimit:     *eventLimit,
		LogEvents:      *logEvents,
		Catalog:        catalog,
		ResponseTimeout: func() time.Duration {
			if responseTO == nil {
				return 5 * time.Second
			}
			return *responseTO
		}(),
		Logger: logger,
	}

	displayAddr := strings.TrimSpace(*listenAddr)
	if strings.HasPrefix(displayAddr, ":") {
		displayAddr = "127.0.0.1" + displayAddr
	}
	if displayAddr != "" {
		logger.Printf("knxproxy: OpenAPI specification available at http://%s/openapi.json", displayAddr)
	}

	if err := proxy.Run(ctx, opts); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Fatalf("knxproxy: %v", err)
	}
}
