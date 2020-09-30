package s3plugin_test

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/urfave/cli"
)

var emptyList = []string{}
const respMsg = `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadOutput>
   <Location>mockValue</Location>
   <Bucket>mockValue</Bucket>
   <Key>mockValue</Key>
   <ETag>mockValue</ETag>
</CompleteMultipartUploadOutput>`
var buf12MB = make([]byte, 1024*1024*12)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "s3_plugin tests")
}

func TestProxyRequest(t *testing.T) {
	fakeS3Proxy := "http://127.0.0.1:8000"

	httpclient := &http.Client{
		Transport: &http.Transport{
			Proxy: func(*http.Request) (*url.URL, error) {
				return url.Parse(fakeS3Proxy)
			},
		},
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		//Endpoint:    aws.String(fakeS3Proxy),
		Credentials: credentials.NewStaticCredentials("123", "123", "123"),
		HTTPClient:  httpclient,
	})

	bucketName := "fakes3"
	keyName := "foo"
	file, _ := ioutil.ReadFile("./s3plugin_test.go")

	buf := bytes.NewBuffer(file)

	// S3 service client the Upload manager will use.
	s3Svc := s3.New(sess)
	uploader := s3manager.NewUploaderWithClient(s3Svc, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024
		u.MaxUploadParts = 2
	})

	upParams := &s3manager.UploadInput{
		Bucket: &bucketName,
		Key:    &keyName,
		Body:   buf,
	}

	result, err := uploader.Upload(upParams)

	log.Println(result, err)
}

func contains(src []string, s string) bool {
	for _, v := range src {
		if s == v {
			return true
		}
	}
	return false
}

func loggingSvc(ignoreOps []string, sess *session.Session) (*s3.S3, *[]string, *[]interface{}) {
	var m sync.Mutex
	partNum := 0
	names := []string{}
	params := []interface{}{}

	if sess == nil {
		sess = unit.Session

	}

	if sess == nil {
		sess = unit.Session
	}
	service := s3.New(sess)
	service.Handlers.Unmarshal.Clear()
	service.Handlers.UnmarshalMeta.Clear()
	service.Handlers.UnmarshalError.Clear()
	service.Handlers.Send.Clear()
	service.Handlers.Send.PushBack(func(r *request.Request) {
		m.Lock()
		defer m.Unlock()

		if !contains(ignoreOps, r.Operation.Name) {
			names = append(names, r.Operation.Name)
			params = append(params, r.Params)
		}

		r.HTTPResponse = &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewReader([]byte(respMsg))),
		}

		switch data := r.Data.(type) {
		case *s3.CreateMultipartUploadOutput:
			data.UploadId = aws.String("UPLOAD-ID")
		case *s3.UploadPartOutput:
			partNum++
			data.ETag = aws.String(fmt.Sprintf("ETAG%d", partNum))
		case *s3.CompleteMultipartUploadOutput:
			data.Location = aws.String("https://location")
			data.VersionId = aws.String("VERSION-ID")
		case *s3.PutObjectOutput:
			data.VersionId = aws.String("VERSION-ID")
		}
	})

	return service, &names, &params
}

func TestUpload(t *testing.T) {
	s3Svc, _, _ := loggingSvc(emptyList, nil)

	uploader := s3manager.NewUploaderWithClient(s3Svc)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String("test_bucket"),
		Key:                  aws.String("test_key"),
		Body:				  bytes.NewReader([]byte("test_data")),
		ContentType:          aws.String("content/type"),
		})
	log.Println(err)
	//Expect(err).ToNot(HaveOccurred())
}

var _ = Describe("s3_plugin tests", func() {
	var pluginConfig *s3plugin.PluginConfig
	BeforeEach(func() {
		pluginConfig = &s3plugin.PluginConfig{
			ExecutablePath: "/tmp/location",
			Options: map[string]string{
				"aws_access_key_id":     "12345",
				"aws_secret_access_key": "6789",
				"bucket":                "bucket_name",
				"folder":                "folder_name",
				"region":                "region_name",
				"endpoint":              "endpoint_name",
			},
		}
	})
	Describe("GetS3Path", func() {
		It("it combines the folder directory with a path that results from removing all but the last 3 directories of the file path parameter", func() {
			folder := "s3/Dir"
			path := "/a/b/c/tmp/datadir/gpseg-1/backups/20180101/20180101082233/backup_file"
			newPath := s3plugin.GetS3Path(folder, path)
			expectedPath := "s3/Dir/backups/20180101/20180101082233/backup_file"
			Expect(newPath).To(Equal(expectedPath))
		})
	})
	Describe("ShouldEnableEncryption", func() {
		It("returns true when no encryption in config", func() {
			delete(pluginConfig.Options, "encryption")
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
		It("returns true when encryption set to 'on' in config", func() {
			pluginConfig.Options["encryption"] = "on"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
		It("returns false when encryption set to 'off' in config", func() {
			pluginConfig.Options["encryption"] = "off"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeFalse())
		})
		It("returns true when encryption set to anything else in config", func() {
			pluginConfig.Options["encryption"] = "random_text"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
	})
	Describe("ValidateConfig", func() {
		It("succeeds when all fields in config filled", func() {
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("succeeds when all fields except endpoint filled in config", func() {
			delete(pluginConfig.Options, "endpoint")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("succeeds when all fields except region filled in config", func() {
			delete(pluginConfig.Options, "region")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("succeeds when all fields except aws_access_key_id and aws_secret_access_key in config", func() {
			delete(pluginConfig.Options, "aws_access_key_id")
			delete(pluginConfig.Options, "aws_secret_access_key")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("sets region to unused when endpoint is used instead of region", func() {
			delete(pluginConfig.Options, "region")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).ToNot(HaveOccurred())
			Expect(pluginConfig.Options["region"]).To(Equal("unused"))
		})
		It("returns error when neither region nor endpoint in config", func() {
			delete(pluginConfig.Options, "region")
			delete(pluginConfig.Options, "endpoint")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no aws_access_key_id in config", func() {
			delete(pluginConfig.Options, "aws_access_key_id")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no aws_secret_access_key in config", func() {
			delete(pluginConfig.Options, "aws_secret_access_key")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no bucket in config", func() {
			delete(pluginConfig.Options, "bucket")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no folder in config", func() {
			delete(pluginConfig.Options, "folder")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
	})
	Describe("Delete", func() {
		var flags *flag.FlagSet

		BeforeEach(func() {
			flags = flag.NewFlagSet("testing flagset", flag.PanicOnError)

			options := make(map[string]string, 0)
			options["region"] = "us-west-2"
			options["aws_access_key_id"] = "myId"
			options["aws_secret_access_key"] = "secret"
			options["bucket"] = "myBucket"
			options["folder"] = "foo/bar"
		})
		It("returns error when timestamp is not provided", func() {
			err := flags.Parse([]string{"myconfigfilepath"})
			Expect(err).ToNot(HaveOccurred())
			context := cli.NewContext(nil, flags, nil)

			err = s3plugin.DeleteBackup(context)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("delete requires a <timestamp>"))
		})
		It("returns error when timestamp does not parse", func() {
			err := flags.Parse([]string{"myconfigfilepath", "badformat"})
			Expect(err).ToNot(HaveOccurred())
			context := cli.NewContext(nil, flags, nil)

			err = s3plugin.DeleteBackup(context)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("delete requires a <timestamp> with format YYYYMMDDHHMMSS, but received: badformat"))
		})
	})
	Describe("Backup", func() {
		It("simple upload test", func() {
			s3Svc, _, _ := loggingSvc(emptyList, nil)
			uploader := s3manager.NewUploaderWithClient(s3Svc)
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket:               aws.String("test_bucket"),
				Key:                  aws.String("test_key"),
				Body:				  bytes.NewReader([]byte("test_data")),
				ContentType:          aws.String("content/type"),
			})
			Expect(err).ToNot(HaveOccurred())
		})
		It("upload using proxy", func() {
			httpclient := &http.Client{
				Transport: &http.Transport{
					Proxy: func(*http.Request) (*url.URL, error) {
						return url.Parse("http://user:pass@proxy:8080")
					},
				},
			}
			var sess = session.Must(session.NewSession(&aws.Config{
				Credentials: credentials.NewStaticCredentials("AKID", "SECRET", "SESSION"),
				Region:      aws.String("mock-region"),
				SleepDelay:  func(time.Duration) {},
				HTTPClient:  httpclient,
			}))
			s3Svc, _, _ := loggingSvc(emptyList, sess)
			uploader := s3manager.NewUploaderWithClient(s3Svc)
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket:               aws.String("test_bucket"),
				Key:                  aws.String("test_key"),
				Body:				  bytes.NewReader([]byte("test_data")),
				ContentType:          aws.String("content/type"),
			})
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
