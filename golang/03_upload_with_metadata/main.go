// ###############################################################################

// Title:        03_upload_with_metadata
// Author:       Marko Hauke
// Date:         2023-12-04
// Description:  Upload Objects with custom metadata

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
	"strings"
	"path/filepath"
	"net/http"
	"encoding/json"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type LoDConfig struct {
	Bucket string   `json:"bucket"`
	Files  string `json:"files"`
}

type Object struct {
	Key string
	Size int
	ModifiedDate time.Time
	Type string
}

//type Objects []Object

var UploadedObjects []Object

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

func getMetaData(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data map[string]string
	dec := json.NewDecoder(file)
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}


func handleFile(s3Client *s3.Client, bucket string) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}

		// Check if the file is not a JSON file 
		if !info.IsDir() && strings.ToLower(filepath.Ext(info.Name())) != ".json" {
			base := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))
			jsonFile := filepath.Join(filepath.Dir(path), base+".JSON")

			// open the file
			file, err := os.Open(path)
			if err != nil {
				log.Printf("Couldn't open file %v to upload. %v\n", path, err)
			}
			defer file.Close()

			// get the metadata from the coresponding JSON file
			metadata, err := getMetaData(jsonFile)
			if err != nil {
				log.Printf("Couldn't get metadata from JSON file %v. %v\n", path, err)
				metadata = nil
			}

			// Upload the image file to S3 bucket and add metadata
			_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:	aws.String(info.Name()),
				Body: file,
				Metadata: metadata,
			})

			// Check that the file was uploaded by head-object API request
			resp, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:	aws.String(info.Name()),
			})
			if err != nil {
				log.Printf("Head request to object %v failed. %v", info.Name(), err)
			}
			
			newObject := Object{
					Key: info.Name(),
					Size: int(*resp.ContentLength),
					ModifiedDate: *resp.LastModified,
					Type: resp.Metadata["type"], 
			}
			UploadedObjects = append(UploadedObjects, newObject)
		}
		return nil
	}
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

	// Correcting for dir structure in go
	lodConfig.Files = fmt.Sprintf("../%v", lodConfig.Files)
	
	s3cfg, err := createS3Config(endpoint, access_key, secret_key)
	if err != nil {
		log.Fatalf("unable to create AWS S3 config, %v", err)
	}

	// create a S3 client
	s3Client := s3.NewFromConfig(s3cfg)

	fileerr := filepath.Walk(lodConfig.Files, handleFile(s3Client, lodConfig.Bucket))

	if fileerr != nil {
		fmt.Printf("error walking the path %v: %v\n", lodConfig.Files, err)
	}

	// // Output of the uploaded objects
	fmt.Println("Uploaded Objects:")
	for _, item := range UploadedObjects {
		fmt.Printf("Name: %s, Last Modified: %v, Size: %d, Type: %s \n", item.Key, item.ModifiedDate, item.Size, item.Type)
		//uploadedFiles.increment(1)
	}

	fmt.Printf("\nObjects: %v\n", len(UploadedObjects))
}
