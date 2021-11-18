//go:build !embedstatic
// +build !embedstatic

package api

import "net/http"

func staticFs() (http.FileSystem, error) {
	return http.Dir("public"), nil
}
