package proxy

import _ "embed"

// OpenAPISpec returns the OpenAPI 3 specification for the KNX proxy service.
func OpenAPISpec() []byte {
	return append([]byte(nil), openAPISpec...)
}

//go:embed openapi.json
var openAPISpec []byte
