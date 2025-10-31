package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/knx-go/knx-go/knx/postgres"
	"github.com/knx-go/knx-go/knx/proxy"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Expose KNX group operations over HTTP",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			applyServeConfig(cmd)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(cmd.Context())
		},
	}

	cmd.Flags().StringVarP(&serveListenAddr, "listen", "l", ":8080", "HTTP listen address")
	cmd.Flags().IntVarP(&serveEventLimit, "event-buffer", "b", 64, "maximum number of events to retain in memory")
	cmd.Flags().BoolVar(&serveLogEvents, "log-events", false, "log inbound KNX group events")
	cmd.Flags().StringVarP(&serveGroupFile, "group-file", "f", "", "path to a KNX group address export (XML)")
	cmd.Flags().DurationVarP(&serveTimeout, "response-timeout", "r", 5*time.Second, "maximum time to wait for group responses")
	cmd.Flags().StringVar(&serveDatabaseURL, "database-url", "", "PostgreSQL connection string used to persist and query events")

	root.AddCommand(cmd)
}

func applyServeConfig(cmd *cobra.Command) {
	if value := strings.TrimSpace(viper.GetString("serve.listen")); value != "" && !flagChanged(cmd, "listen") {
		serveListenAddr = value
	}
	if viper.IsSet("serve.event_buffer") && !flagChanged(cmd, "event-buffer") {
		serveEventLimit = viper.GetInt("serve.event_buffer")
	}
	if viper.IsSet("serve.log_events") && !flagChanged(cmd, "log-events") {
		serveLogEvents = viper.GetBool("serve.log_events")
	}
	if value := strings.TrimSpace(viper.GetString("serve.group_file")); value != "" && !flagChanged(cmd, "group-file") {
		serveGroupFile = value
	}
	if viper.IsSet("serve.response_timeout") && !flagChanged(cmd, "response-timeout") {
		if duration, ok := normalizeDuration(viper.Get("serve.response_timeout")); ok {
			serveTimeout = duration
		}
	}
	if value := strings.TrimSpace(viper.GetString("serve.database_url")); value != "" && !flagChanged(cmd, "database-url") {
		serveDatabaseURL = value
	}
}

func runServe(ctx context.Context) error {
	catalog, err := loadCatalog(serveGroupFile)
	if err != nil {
		return err
	}
	if catalog != nil {
		fmt.Printf("Loaded %d group addresses from %s\n", len(catalog.Groups()), strings.TrimSpace(serveGroupFile))
	}

	gatewayAddr := fmt.Sprintf("%s:%s", server, port)
	logger := log.New(os.Stdout, "", log.LstdFlags)

	parent := ctx
	if parent == nil {
		parent = context.Background()
	}

	runCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := postgres.OpenStore(serveDatabaseURL)
	if err != nil {
		return err
	}
	if store != nil {
		defer store.Close()
		fmt.Println("Database history enabled")
	}

	opts := proxy.Options{
		GatewayAddress:  gatewayAddr,
		ListenAddress:   serveListenAddr,
		EventLimit:      serveEventLimit,
		LogEvents:       serveLogEvents,
		Catalog:         catalog,
		ResponseTimeout: serveTimeout,
		Logger:          logger,
		Store:           store,
	}

	displayAddr := strings.TrimSpace(serveListenAddr)
	if strings.HasPrefix(displayAddr, ":") {
		displayAddr = "127.0.0.1" + displayAddr
	}
	if displayAddr != "" {
		fmt.Printf("OpenAPI specification available at http://%s/openapi.json\n", displayAddr)
	}

	if err := proxy.Run(runCtx, opts); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	return nil
}
