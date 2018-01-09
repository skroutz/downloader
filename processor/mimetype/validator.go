package mimetype

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/rakyll/magicmime"
)

const MimeTypeValidationThreshold = 1024

// Validator checks its buffer's mime type against the provided checks.
// It holds a reference to a mime type decoder.
type Validator struct {
	buffer  *bytes.Buffer
	decoder *magicmime.Decoder

	// holds all checks to be done against the bytes written to the buffer
	checks []Check
}

// ErrMimeTypeMismatch is a custom error exposing info on the given failed check.
type ErrMimeTypeMismatch struct {
	check Check
	found string
}

// Check holds info on a specific test check is a string/mimetype to be matched
// negate indicates if the check should be handled as a blacklist or a whitelist.
type Check struct {
	check  string
	negate bool
}

// Error returns the error string for the current ErrMimeTypeMismatch.
func (e ErrMimeTypeMismatch) Error() string {
	if e.check.negate {
		return fmt.Sprintf("Expected mime-type not to be (%s) found - (%s)", e.check.check, e.found)
	} else {
		return fmt.Sprintf("Expected mime-type to be (%s), found (%s)", e.check.check, e.found)
	}
}

// New constructs a new validator.
func New() (*Validator, error) {
	decoder, err := magicmime.NewDecoder(magicmime.MAGIC_MIME_TYPE)
	if err != nil {
		return nil, err
	}

	// Buffer's internal []byte slice will be grown to 2*size + MinRead during buff.ReadFrom()
	// anyway so we allocate it in a single step right from the start.
	// See https://golang.org/pkg/bytes/#Buffer.ReadFrom source.
	buf := bytes.NewBuffer(make([]byte, 0, 2*MimeTypeValidationThreshold+bytes.MinRead))
	return &Validator{decoder: decoder, buffer: buf}, nil
}

func (v *Validator) extractListsFromString(checks string) {
	v.checks = nil
	for _, c := range strings.Split(checks, ",") {
		c = strings.TrimSpace(c)
		if strings.HasPrefix(c, "!") {
			v.checks = append(v.checks, Check{check: c[1:], negate: true})
			continue
		}
		v.checks = append(v.checks, Check{check: c, negate: false})
	}
}

// ValidateMimeTypePattern validates that the checks extracted from pattern
// can be used as glob patterns against mime types.
func ValidateMimeTypePattern(pattern string) error {
	var err error
	for _, c := range strings.Split(pattern, ",") {
		c = strings.TrimSpace(c)
		if strings.HasPrefix(c, "!") {
			_, err = filepath.Match(c[1:], "*")
		} else {
			_, err = filepath.Match(c, "*")
		}
		if err != nil {
			return fmt.Errorf("Invalid MimeType Pattern, %q", c)
		}
		continue
	}
	return nil
}

// Reset resets the current validatorby reinitializing all checks based on the given pattern.
func (v *Validator) Reset(expectedMimePattern string) {
	v.extractListsFromString(expectedMimePattern)
	v.buffer.Reset()
}

// Read takes an io.Reader as an argument, tries to read MimeTypeValidationThreshold of input
// bytes, or fewer if the request is shorter. It then performs mime type checks against its buffer.
// Any r.Read() errors are returned verbatim.
func (v *Validator) Read(r io.Reader) error {
	_, err := v.buffer.ReadFrom(io.LimitReader(r, MimeTypeValidationThreshold))
	if err != nil {
		return err
	}

	return v.CheckBuffer(v.buffer.Bytes())
}

// CheckBuffer performs mime types checks against the provided byte slice.
func (v *Validator) CheckBuffer(p []byte) error {
	var mime string
	var err error
	// decoder.TypeByBuffer() panics with empty slices. We guard against
	// that and manually return "application/x-empty" which is what libmagic
	// returns on empty buffers until this is handled upstream.
	if len(p) > 0 {
		mime, err = v.decoder.TypeByBuffer(p)
		if err != nil {
			return err
		}
	} else {
		mime = "application/x-empty"
	}

	for _, check := range v.checks {
		if !check.IsValid(mime) {
			return ErrMimeTypeMismatch{check, mime}
		}
	}

	return nil
}

// Close closes the internal mime-type decoder.
func (v *Validator) Close() {
	v.decoder.Close()
}

// IsValid validates the given mime string against the current check.
func (c Check) IsValid(mime string) bool {
	// Only error here can be ErrBadPattern, checks for which are place in job creation.
	match, _ := filepath.Match(c.check, mime)
	return match != c.negate
}
