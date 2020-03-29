package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	fmt.Println("Yo")
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sqs.New(sess)

	// URL to our queue
	qURL := "https://us-west-2.queue.amazonaws.com/943240146135/mess"

	for {
		result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            &qURL,
			MaxNumberOfMessages: aws.Int64(1),
			VisibilityTimeout:   aws.Int64(20), // 20 seconds
			WaitTimeSeconds:     aws.Int64(0),
		})

		if err != nil {
			fmt.Println("Error", err)
			return
		}
		downloader := s3manager.NewDownloader(sess)

		buff := &aws.WriteAtBuffer{}

		//https://github.com/aws/aws-lambda-go/blob/master/events/s3.go
		var event events.S3Event

		// loop over the events in the SQS payload
		for _, r := range result.Messages {
			json.Unmarshal([]byte(aws.StringValue(r.Body)), &event)

			for _, record := range event.Records {
				fmt.Printf("s3://%s/%s\n", record.S3.Bucket.Name, record.S3.Object.Key)

				// Download the file from S3 into buffer
				numBytes, err := downloader.Download(buff,
					&s3.GetObjectInput{
						Bucket: aws.String(record.S3.Bucket.Name),
						Key:    aws.String(record.S3.Object.Key),
					})
				if err != nil {
					log.Fatalf("Unable to download item %q, %v", record.S3.Object.Key, err)
				}
				fmt.Println(numBytes)
				fmt.Printf("%s\n", buff.Bytes())

			}
		}

		if len(result.Messages) == 0 {
			fmt.Println("Received no messages")
			return
		}

		_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &qURL,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			fmt.Println("Delete Error", err)
			return
		}

		fmt.Println("Message Deleted")
	}

}

func GetWebhook() {

}
