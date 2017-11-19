// +build embedstatic
//go:generate statik -src=../public -f

package api

import (
	"net/http"

	"github.com/rakyll/statik/fs"
	// Import the statik generated package
	_ "golang.skroutz.gr/skroutz/downloader/api/statik"
)

func staticFs() (http.FileSystem, error) {
	return fs.New()
}
