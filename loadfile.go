package loadfile

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Loader struct {
	types    map[*regexp.Regexp]TypeLoader
	fallback TypeLoader
}

// Load fetches a file and unmarshals into a struct. JSON, XML and YML encoding
// supported by filename extension. Tries JSON if none match.
func (l *Loader) Load(filename string, into interface{}) error {
	reader, err := l.GetReader(filename)
	if err != nil {
		return err
	}
	if readCloser, ok := reader.(io.Closer); ok {
		defer readCloser.Close()
	}
	fileDotParts := strings.Split(filename, ".")
	fileExtension := fileDotParts[len(fileDotParts)-1]
	switch strings.ToLower(fileExtension) {
	case "json":
		return json.NewDecoder(reader).Decode(into)
	case "xml":
		return xml.NewDecoder(reader).Decode(into)
	case "yml", "yaml":
		b, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		return yaml.Unmarshal(b, into)
	}

	return json.NewDecoder(reader).Decode(into)
}

func (l *Loader) getReaderGetter(filename string) TypeLoader {
	for re, getter := range l.types {
		if re.MatchString(filename) {
			return getter
		}
	}
	return l.fallback
}

func (l *Loader) GetReader(filename string) (io.Reader, error) {
	rg := l.getReaderGetter(filename)
	if rg == nil {
		return nil, ErrorNoReader
	}
	return rg.GetReader(filename)
}

func (l *Loader) GetReadCloser(filename string) (io.ReadCloser, error) {
	r, err := l.GetReader(filename)
	if err != nil {
		return nil, err
	}
	if readCloser, ok := r.(io.ReadCloser); ok {
		return readCloser, nil
	}
	return ioutil.NopCloser(r), nil
}

// ErrorNoReader is returned when no loader regex matches
var ErrorNoReader = errors.New("No Loader matched the given filename")

var reS3Filename = regexp.MustCompile(`^s3:\/\/([^\/]+)\/(.*)$`)

// TypeLoader returns an io.Reader for the given filename. If it returns an
// io.ReadCloser, Loader.Load will close it.
type TypeLoader interface {
	GetReader(filename string) (io.Reader, error)
}

// S3Loader fetches a file from an AWS S3 bucket using default AWS credentials.
// Supports 'shared config state', i.e., AWS_SDK_LOAD_CONFIG is forced to true,
// meaning AWS_PROFILE works
type S3Loader struct{}

func (S3Loader) GetReader(filename string) (io.Reader, error) {

	parts := reS3Filename.FindStringSubmatch(filename)
	if len(parts) != 3 {
		return nil, errors.New("Impossible bad match passed to S3Loader")
	}
	bucket := parts[1]
	key := parts[2]

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3Conn := s3.New(sess)
	obj, err := s3Conn.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return obj.Body, nil
}

// FileLoader blindly uses os.Open
type FileLoader struct{}

func (FileLoader) GetReader(filename string) (io.Reader, error) {
	return os.Open(filename)
}

// DefaultLoader implements all implemented types
var DefaultLoader = &Loader{
	types: map[*regexp.Regexp]TypeLoader{
		reS3Filename: S3Loader{},
	},
	fallback: &FileLoader{},
}

// Load a file into a struct, using the default loader
func Load(filename string, into interface{}) error {
	return DefaultLoader.Load(filename, into)
}

func GetReader(filename string) (io.Reader, error) {
	return DefaultLoader.GetReader(filename)
}

func GetReadCloser(filename string) (io.ReadCloser, error) {
	return DefaultLoader.GetReadCloser(filename)
}
