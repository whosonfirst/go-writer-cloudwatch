package cloudwatch

// see also: https://github.com/boxfuse/cloudwatchlogs-agent/blob/master/logger.go

import (
	"fmt"
	"github.com/aaronland/go-string/dsn"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/whosonfirst/go-whosonfirst-aws/session"
	_ "log"
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

	dsn_map, err := dsn.StringToDSNWithKeys(cw_dsn, "region", "credentials", "group")

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

	stream_name, ok := dsn_map["stream"]

	if !ok {
		now := time.Now()
		stream_name = fmt.Sprintf("%s-%d", group_name, now.Unix())
	}

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

// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html

func (wr CloudWatchWriter) Write(msg []byte) (int, error) {

	now := time.Now()
	ts := now.UnixNano() / int64(time.Millisecond)

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

	token, err := wr.nextSequenceToken()

	if err != nil {
		return 0, err
	}

	if token != "" {
		req.SequenceToken = aws.String(token)
	}

	_, err = wr.service.PutLogEvents(req)

	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (wr CloudWatchWriter) Close() error {
	return nil
}

func (wr CloudWatchWriter) nextSequenceToken() (string, error) {

	req := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(wr.group),
		LogStreamNamePrefix: aws.String(wr.stream),
		Descending:          aws.Bool(true),
		Limit:               aws.Int64(1),
	}

	rsp, err := wr.service.DescribeLogStreams(req)

	if err != nil {
		return "", err
	}

	if len(rsp.LogStreams) == 0 {
		return "", nil
	}

	first := rsp.LogStreams[0]

	if first.UploadSequenceToken == nil {
		return "", nil
	}

	return *first.UploadSequenceToken, nil
}
