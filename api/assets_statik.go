//go:build embedstatic
// +build embedstatic

//go:generate statik -src=../public -f

package api

import (
	"net/http"

	"github.com/rakyll/statik/fs"
	// Import the statik generated package
	_ "github.com/skroutz/downloader/api/statik"
)

func staticFs() (http.FileSystem, error) {
	return fs.New()
}
