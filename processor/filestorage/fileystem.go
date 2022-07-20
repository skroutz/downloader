package filestorage

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

type FileSystem struct {
	RootDir string
}

func NewFileSystem(rootdir string) *FileSystem {
	err := os.MkdirAll(rootdir, os.FileMode(0755))
	if err != nil {
		return nil
	}
	return &FileSystem{RootDir: rootdir}
}

// StoreFile stores a file to the filesystem storage and removes the source
func (fs FileSystem) StoreFile(srcpath string, destpath string) error {
	fulldestpath := path.Join(fs.RootDir, destpath)
	err := os.MkdirAll(filepath.Dir(fulldestpath), os.FileMode(0755))
	if err != nil {
		return err
	}

	err = os.Rename(srcpath, fulldestpath)
	if err != nil {
		fsrc, err := os.Open(srcpath)
		if err != nil {
			return err
		}
		defer fsrc.Close()

		fdest, err := os.Create(fulldestpath)
		if err != nil {
			return err
		}
		defer fdest.Close()

		_, err = io.Copy(fdest, fsrc)
		if err != nil {
			return err
		}
		os.Remove(srcpath)
	}

	return nil
}

// StoreFileWithTags simply calls StoreFiles since we can't handle tags in this
// backend
func (fs FileSystem) StoreFileWithMetadata(srcpath string, destpath string, metadata map[string]interface{}) error {
	fmt.Fprintf(os.Stderr, "[WARN] StoreFileWithTags called from filesystem backend. Metadata are ignored\n")
	return fs.StoreFile(srcpath, destpath)
}

// DeleteFile removes a file from the filesystem storage
func (fs FileSystem) DeleteFile(filepath string) error {
	abspath := path.Join(fs.RootDir, filepath)
	err := os.Remove(abspath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// FileExists returns true if the file exists, false otherwise
func (fs FileSystem) FileExists(filepath string) bool {
	abspath := path.Join(fs.RootDir, filepath)
	_, err := os.Stat(abspath)
	return err == nil
}
