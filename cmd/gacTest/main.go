package main

import (
	"fmt"
	"os"

	"github.com/knx-go/knx-go/knx/gac"
)

func main() {
	catalog, err := gac.Import(os.Stdin)
	if err != nil {
		fmt.Printf("failed to import stdin: %s", err)
		return
	}

	if catalog == nil {
		fmt.Printf("gae: catalog is nil")
		return
	}

	if err := catalog.Export(os.Stdout); err != nil {
		fmt.Printf("gae: failed to export catalog to stdout: %s", err)
		return
	}
}
