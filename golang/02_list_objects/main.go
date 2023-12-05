// ###############################################################################

// Title:        02_list_objects
// Author:       Marko Hauke
// Date:         2023-12-04
// Description:  List all Objects in a bucket

// SDK:      	 aws-sdk-go-v2	
		
// URLs:         https://docs.netapp.com/us-en/storagegrid-117/
//               https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3
//               https://docs.aws.amazon.com/code-library/latest/ug/go_2_s3_code_examples.html			

// ###############################################################################


package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type LoDConfig struct {
	Bucket string   `json:"bucket"`
	Files  string `json:"files"`
}

// load environment variables for endpoint, access_key and secret_key
func loadEnvVars() (string, string, string, error) {
	endpoint := os.Getenv("ENDPOINT")
	if endpoint == "" {
		return "", "", "", fmt.Errorf("ENDPOINT not set")
	}

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKeyID == "" {
		return "", "", "", fmt.Errorf("AWS_ACCESS_KEY_ID not set")
	}

	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretAccessKey == "" {
		return "", "", "", fmt.Errorf("AWS_SECRET_ACCESS_KEY not set")
	}

	return endpoint, accessKeyID, secretAccessKey, nil
}

// load the lod variables from json file.
func loadConfig(filename string) (LoDConfig, error) {
	file, err := os.Open(filename)
	if err != nil {
		return LoDConfig{}, err
	}
	defer file.Close()

	var config LoDConfig
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return LoDConfig{}, err
	}

	return config, nil
}

// createS3Config uses s3config to build the AWS S3 config
// It takes a endpoint, access_key and secret_key as arguments and returns an AWS Config instance and an error.
func createS3Config(endpoint, access_key, secret_key string) (aws.Config, error) {

	const defaultRegion = "us-east-1"
	var no_ssl_verify bool = true

	// create a custom resolver for the StorageGRID endpoint
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               endpoint,
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
		return aws.Config{}, fmt.Errorf("Unable to load SDK config, %v", err)
	}
	return cfg, nil
}


func main() {

	//set config file variables
	config_file := "../../config.json"

	endpoint, access_key, secret_key, err := loadEnvVars()
	if err != nil {
		log.Fatalf("Failed to get environment variables: %v", err)
	}

	lodConfig, err := loadConfig(config_file)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// unsetting env variable AWS_CA_BUNDLE as it causes issues
	os.Unsetenv("AWS_CA_BUNDLE")

	found_objects := 0

	s3cfg, err := createS3Config(endpoint, access_key, secret_key)
	if err != nil {
		log.Fatalf("unable to create AWS S3 config, %v", err)
	}

	// create a S3 client
	s3Client := s3.NewFromConfig(s3cfg)

	// get all objects from bucket
	res, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(lodConfig.Bucket),
		Prefix: aws.String(""),
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
