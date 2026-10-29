// Package esresponse holds pure predicates over Elasticsearch HTTP error
// responses that more than one owner package needs (enrich, managed): they
// read a status code and raw body and never perform I/O or log.
package esresponse

import (
	"bytes"
	"net/http"
)

// IsUnsupportedEnrichAPI reports a generic 404 that carries no Elasticsearch
// error object: a proxy or routing problem in front of the enrich API, not a
// missing policy (resource_not_found_exception) or another ES error.
func IsUnsupportedEnrichAPI(statusCode int, body []byte) bool {
	if statusCode != http.StatusNotFound {
		return false
	}

	if bytes.Contains(body, []byte(`"error":{`)) {
		return false
	}

	return !bytes.Contains(body, []byte("resource_not_found_exception"))
}

// HasErrorType reports whether body contains an Elasticsearch error of
// errorType (a literal "type":"<errorType>" match).
func HasErrorType(body []byte, errorType string) bool {
	return bytes.Contains(body, []byte(`"type":"`+errorType+`"`))
}
