package proxy

import (
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/knx-go/knx-go/knx"
	"github.com/knx-go/knx-go/knx/dpt"
	"github.com/knx-go/knx-go/knx/gac"
)

func TestNewServerAppliesDefaults(t *testing.T) {
	srv := newServer((*knx.GroupTunnel)(nil), log.New(io.Discard, "", 0), 0, false, nil, 0, nil)
	if srv.eventLimit != 64 {
		t.Fatalf("eventLimit = %d, want 64", srv.eventLimit)
	}
	if cap(srv.events) != 64 {
		t.Fatalf("events capacity = %d, want 64", cap(srv.events))
	}
	if srv.responseTimeout != 5*time.Second {
		t.Fatalf("responseTimeout = %v, want 5s", srv.responseTimeout)
	}
}

func TestDescribeDatapoints(t *testing.T) {
	group := &gac.Group{
		DPTs: []dpt.DataPointType{("1.001"), ("9.001")},
	}
	data := dpt.DPT_1001(true).Pack()
	result := describeDatapoints(group, data)
	if !strings.Contains(result, "1.001=On") {
		t.Fatalf("describeDatapoints() = %q", result)
	}
	if !strings.Contains(result, "9.001=<decode error: given application data has invalid length>") {
		t.Fatalf("describeDatapoints() = %q", result)
	}
	if got := describeDatapoints(nil, data); got != "" {
		t.Fatalf("describeDatapoints(nil) = %q, want empty", got)
	}
	if got := describeDatapoints(group, nil); got != "" {
		t.Fatalf("describeDatapoints(no data) = %q, want empty", got)
	}
}

const sampleCatalog = `<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<GroupAddress-Export xmlns="http://knx.org/xml/ga-export/01">
  <GroupRange Name="Root" RangeStart="1" RangeEnd="512">
    <GroupAddress Name="light" Address="1" DPTs="DPST-1-1" />
  </GroupRange>
</GroupAddress-Export>`

func TestParseDestination(t *testing.T) {
	catalog, err := gac.Import(strings.NewReader(sampleCatalog))
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}
	srv := newServer((*knx.GroupTunnel)(nil), log.New(io.Discard, "", 0), 1, false, catalog, 0, nil)

	addr, name, fromCatalog, err := srv.parseDestination("light")
	if err != nil {
		t.Fatalf("parseDestination(name) error = %v", err)
	}
	if name != "light" || !fromCatalog {
		t.Fatalf("parseDestination(name) = (%s, %t)", name, fromCatalog)
	}
	if addr.String() != "0/0/1" {
		t.Fatalf("parseDestination(name) addr = %s, want 0/0/1", addr)
	}

	addr, name, fromCatalog, err = srv.parseDestination("0/0/1")
	if err != nil {
		t.Fatalf("parseDestination(address) error = %v", err)
	}
	if name != "light" || fromCatalog {
		t.Fatalf("parseDestination(address) = (%s, %t)", name, fromCatalog)
	}
	if addr.String() != "0/0/1" {
		t.Fatalf("parseDestination(address) addr = %s, want 0/0/1", addr)
	}

	if _, _, _, err := srv.parseDestination("invalid/addr"); err == nil {
		t.Fatal("parseDestination() expected error for invalid input")
	}
}

func TestLooksLikeName(t *testing.T) {
	if looksLikeName("group/name") {
		t.Fatal("looksLikeName() incorrectly accepted path-like value")
	}
	if !looksLikeName("livingroom") {
		t.Fatal("looksLikeName() rejected alpha value")
	}
	if looksLikeName("123.456") {
		t.Fatal("looksLikeName() accepted dotted address")
	}
}
