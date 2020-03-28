package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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

		//https://github.com/aws/aws-lambda-go/blob/master/events/s3.go
		for _, r := range result.Messages {
			var event events.S3Event
			json.Unmarshal([]byte(aws.StringValue(r.Body)), &event)

			for _, record := range event.Records {
				fmt.Printf("s3://%s/%s\n", record.S3.Bucket.Name, record.S3.Object.Key)
			}
		}

		if len(result.Messages) == 0 {
			fmt.Println("Received no messages")
			return
		}

		resultDelete, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &qURL,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			fmt.Println("Delete Error", err)
			return
		}

		fmt.Println("Message Deleted", resultDelete)
	}

}
