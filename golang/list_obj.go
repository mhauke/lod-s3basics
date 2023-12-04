package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {

	// set variables for region and endpoint and create a customer endpoint resolver
	const defaultRegion = "us-east-1"
	hostAddress := "https://dc1-g1.demo.netapp.com:10443"
	no_ssl_verify := true
	access_key := "4KLRZ8WUJLFYK1YYH3UN"
	secret_key := "OfOKsDEsCZ7TV5UZv2Uks4M8ShcgG02YIU4ZezmE"
	bucketName := "ebm-demo-s3select"
	prefixName := ""

	// unsetting env variable AWS_CA_BUNDLE as it causes issues
	os.Unsetenv("AWS_CA_BUNDLE")

	found_objects := 0

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               hostAddress,
			SigningRegion:     defaultRegion,
			HostnameImmutable: true,
		}, nil
	})

	// create HTTP client to skip SSL certificate verify
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: no_ssl_verify},
	}
	client := &http.Client{Transport: tr}

	// create a Default config with credentials provider for access & secret key
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultRegion),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access_key, secret_key, "")),
		config.WithHTTPClient(client),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// create a S3 client
	s3Client := s3.NewFromConfig(cfg)

	// get all objects from bucket
	res, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefixName),
	})
	if err != nil {
		fmt.Printf("Couldn't retrieve bucket items: %v", err)
		return
	}

	// Output list of objects
	for _, item := range res.Contents {
		fmt.Printf("Name: %s, Last Modified: %s, Size: %d\n", *item.Key, *item.LastModified, item.Size)
		found_objects += 1
	}

	fmt.Printf("Found Objects: %d\n", found_objects)

}
