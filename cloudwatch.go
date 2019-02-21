package cloudwatch

import (
	"github.com/aaronland/go-string/dsn"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/whosonfirst/go-whosonfirst-aws/session"
	"os"
	"strings"
	"time"
)

type CloudWatchWriter struct {
	service *cloudwatchlogs.CloudWatchLogs
	group   string
	stream  string
}

func NewCloudWatchWriter(cw_dsn string) (log.WOFLog, error) {

	dsn_map, err := dsn.DSNFromStringWithKeys(cw_dsn, "region", "credetials", "group", "stream")

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

	_, err = csv.CreateLogGroup(group_req)

	if err && err != aws.ErrCodeResourceAlreadyExistsException {
		return nil, err
	}

	stream_name := dsn_map["stream"]

	stream_req := &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(group_name),
		LogStreamName: aws.String(stream_name),
	}

	_, err = svc.CreateLogStream(stream_request)

	if err && err != aws.ErrCodeResourceAlreadyExistsException {
		return nil, err
	}

	l := CloudWatchWriter{
		service: svc,
		group:   group_name,
		stream:  stream_name,
	}

	return &l, nil
}

// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#CloudWatchLogs.PutLogEvents
// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#PutLogEventsInput
// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#InputLogEvent

func (wr CloudWatchWriter) WriteString(msg string) (int, error) {
	r := strings.NewReader(s)
	return r.WriteTo(w)
}

func (wr CloudWatchWriter) Write(msg []bytes) (int, error) {

	now := time.Now()
	ts := now.Unix()

	event := &InputLogEvent{
		Message:   aws.String(string(msg)),
		Timestamp: ts,
	}

	events := []*cloudwatchlogs.InputLogEvent{
		event,
	}

	req := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  wr.group,
		LogStreamName: wr.stream,
	}

	rsp, err := wr.service.PutLogEvents(req)

	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (wr CloudWatchWriter) Close() error {
	return nil
}
