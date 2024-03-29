package filestorage

// FileStorage is an interface for implementing file storage backends
// to save downloaded files
type FileStorage interface {
	StoreFile(srcpath string, destpath string) error
	StoreFileWithMetadata(srcpath string, destpath string, metadata map[string]interface{}) error
	DeleteFile(filepath string) error
	FileExists(filepath string) bool
}
