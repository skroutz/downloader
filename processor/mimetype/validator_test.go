package mimetype

import (
	"io"
	"log"
	"os"
	"testing"
	"testing/iotest"
)

var validator *Validator

func init() {
	var err error
	validator, err = New()
	if err != nil {
		log.Println("Could not create validator:", err)
		os.Exit(1)
	}
}

func TestWhitelist(t *testing.T) {
	validator.Reset("image/jpeg")

	in, err := os.Open("../../testdata/load-test.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	if err = validator.Read(in); err != nil {
		t.Fatal(err)
	}
}

func TestBlacklistOnly(t *testing.T) {
	validator.Reset("!image/vnd.adobe.photoshop,!image/png")

	in, err := os.Open("../../testdata/load-test.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	if err = validator.Read(in); err != nil {
		t.Fatal(err)
	}
}

func TestMultipleWrites(t *testing.T) {
	validator.Reset("image/jpeg")

	in, err := os.Open("../../testdata/load-test.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	testReader := iotest.OneByteReader(in)

	if err = validator.Read(testReader); err != nil {
		t.Fatal(err)
	}
}

func TestImageSmallerThanThreshold(t *testing.T) {
	// intentional mime type mismatch
	validator.Reset("!image/png")

	in, err := os.Open("../../testdata/tiny.png")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	// We should get an error
	if err = validator.Read(in); err == nil {
		t.Fatal(err)
	}
}

type unexpectedReader struct{}

func (p unexpectedReader) Read(buf []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestUnexpectedEOF(t *testing.T) {
	validator.Reset("image/png")

	var in unexpectedReader
	err := validator.Read(in)
	if err == nil {
		t.Fatal("error expected")
	}

	// We shouldn't be happy with a mimetype mismatch error since
	// we hit a read error first.
	if _, ok := err.(ErrMimeTypeMismatch); ok {
		t.Fatal("Unexpected mimetype missmatch error")
	}
}

func TestPatternValidation(t *testing.T) {
	tc := map[string]bool{
		"[]a]":                               false,
		"\\":                                 false,
		"":                                   true,
		"image/*":                            true,
		"!application/xml":                   true,
		"!image/vnd.adobe.photoshop,image/*": true,
	}

	for mime, expected := range tc {
		err := ValidateMimeTypePattern(mime)
		valid := err == nil
		if expected != valid {
			t.Fatal(mime, err)
		}
	}
}

func TestCheck(t *testing.T) {
	mime := "image/vnd.adobe.photoshop"
	check := Check{"image/vnd.adobe.photoshop", true}
	if check.IsValid(mime) {
		t.Fatal("Should be invalid")
	}
}
