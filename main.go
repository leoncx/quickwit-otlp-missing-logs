// Copyright 2025 Maximilien Bersoult.
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-faker/faker/v4"
	flag "github.com/spf13/pflag"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	SeverityNumber = []logspb.SeverityNumber{
		logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG,
		logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
		logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
	}
)

func main() {
	var useHTTP = flag.Bool("http", false, "Send to HTTP protobuf, otherwise to gRPC")
	var endpoint = flag.String("endpoint", "localhost:7281", "OTLP endpoint to send metrics to")
	var controlFilePath = flag.String("control-file", "", "Path to the control file")
	flag.Parse()

	var svc collogspb.LogsServiceClient
	if !*useHTTP {
		conn, err := grpc.NewClient(
			*endpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to connect to gRPC endpoint: %v", err)
		}
		defer conn.Close()
		svc = collogspb.NewLogsServiceClient(conn)
	}

	controlFile := collogspb.ExportLogsServiceRequest{
		ResourceLogs: make([]*logspb.ResourceLogs, 0),
	}

	totalLines := 0
	for i := 0; i < 10; i++ {
		data := collogspb.ExportLogsServiceRequest{
			ResourceLogs: make([]*logspb.ResourceLogs, 0),
		}
		for j := 0; j < 2; j++ {
			now := time.Now()
			// time.Sleep(10 * time.Nanosecond)
			lvl := SeverityNumber[i%len(SeverityNumber)]
			resourceLogs := &logspb.ResourceLogs{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "service.name",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "test-service",
								},
							},
						},
						{
							Key: "service.version",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "0.1.0",
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano:         uint64(now.UnixNano()),
								ObservedTimeUnixNano: uint64(now.UnixNano()),
								SeverityNumber:       lvl,
								SeverityText:         lvl.String(),
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: faker.Sentence(),
										// StringValue: "This is a test log message",
									},
								},
							},
						},
					},
				},
			}
			data.ResourceLogs = append(data.ResourceLogs, resourceLogs)
			controlFile.ResourceLogs = append(
				controlFile.ResourceLogs,
				resourceLogs,
			)
		}

		totalLines += len(data.ResourceLogs)

		if *useHTTP {
			bodyBytes, err := proto.Marshal(&data)
			if err != nil {
				log.Fatalf("failed to marshal data: %v", err)
			}
			resp, err := http.DefaultClient.Post(
				*endpoint,
				"application/x-protobuf",
				bytes.NewReader(bodyBytes),
			)
			if err != nil {
				log.Fatalf("failed to send data: %v", err)
			}
			if resp.StatusCode != http.StatusOK &&
				resp.StatusCode != http.StatusNoContent {
				log.Fatalf("unexpected status code: %d", resp.StatusCode)
			}
		} else {
			_, err := svc.Export(context.Background(), &data)
			if err != nil {
				log.Fatalf("failed to send data: %v", err)
			}
		}
		// time.Sleep(5 * time.Second)
	}

	if *controlFilePath != "" {
		control, err := json.MarshalIndent(&controlFile, "", "  ")
		if err != nil {
			log.Fatalf("failed to marshal control file: %v", err)
		}
		if err = os.WriteFile(*controlFilePath, control, 0644); err != nil {
			log.Fatalf("failed to write control file: %v", err)
		} else {
			log.Println("Control file written to", *controlFilePath)
		}
	}
	println("Sent", totalLines, "log lines to", *endpoint)
}
