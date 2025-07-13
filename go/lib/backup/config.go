package backup

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func GetMinioConfig(url string) *aws.Config {
	const defaultRegion = "us-test-1"
	// TODO: deprecated function
	staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               url,
			SigningRegion:     defaultRegion,
			HostnameImmutable: true,
		}, nil
	})

	cfg := &aws.Config{
		Region:           defaultRegion,
		Credentials:      credentials.NewStaticCredentialsProvider("minio", "minio123", ""),
		EndpointResolver: staticResolver,
	}
	return cfg
}

func GetS3Config() *aws.Config {
	// These should all be the same as what the AWS SDK defaults to, we just want to explicitly _only_
	// look at these.
	// TODO: there's gotta be a way to set up a custom credential chain so we don't have to do this
	// manually.
	region := os.Getenv("AWS_REGION")
	key := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	session := os.Getenv("AWS_SESSION_TOKEN")

	cfg := &aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			key,
			secret,
			session,
		),
	}
	return cfg
}
