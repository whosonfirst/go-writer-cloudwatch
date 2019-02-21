package cloudwatch

import (
	"github.com/aaronland/go-string/dsn"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/whosonfirst/go-whosonfirst-aws/session"
	"log"
	"time"
)

type CloudWatchWriter struct {
	service *cloudwatchlogs.CloudWatchLogs
	group   string
	stream  string
}

func IsAlreadyExistsError(err error) bool {

	aws_err := err.(awserr.Error)

	if aws_err.Code() == "ResourceAlreadyExistsException" {
		return true
	}

	return false
}

func NewCloudWatchWriter(cw_dsn string) (*CloudWatchWriter, error) {

	dsn_map, err := dsn.StringToDSNWithKeys(cw_dsn, "region", "credentials", "group", "stream")

	if err != nil {
		return nil, err
	}

	cw_sess, err := session.NewSessionWithDSN(cw_dsn)

	if err != nil {
		return nil, err
	}

	svc := cloudwatchlogs.New(cw_sess)

	group_name := dsn_map["group"]

	group_req := &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(group_name),
	}

	_, err = svc.CreateLogGroup(group_req)

	if err != nil && !IsAlreadyExistsError(err) {
		return nil, err
	}

	stream_name := dsn_map["stream"]

	stream_req := &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(group_name),
		LogStreamName: aws.String(stream_name),
	}

	_, err = svc.CreateLogStream(stream_req)

	if err != nil && !IsAlreadyExistsError(err) {
		return nil, err
	}

	wr := CloudWatchWriter{
		service: svc,
		group:   group_name,
		stream:  stream_name,
	}

	return &wr, nil
}

// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#CloudWatchLogs.PutLogEvents
// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#PutLogEventsInput
// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#InputLogEvent

func (wr CloudWatchWriter) Write(msg []byte) (int, error) {

	now := time.Now()
	ts := now.Unix()

	event := &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(string(msg)),
		Timestamp: aws.Int64(ts),
	}

	events := []*cloudwatchlogs.InputLogEvent{
		event,
	}

	req := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(wr.group),
		LogStreamName: aws.String(wr.stream),
	}

	rsp, err := wr.service.PutLogEvents(req)

	if err != nil {
		return 0, err
	}

	log.Println(rsp)

	return 0, nil
}

func (wr CloudWatchWriter) Close() error {
	return nil
}
