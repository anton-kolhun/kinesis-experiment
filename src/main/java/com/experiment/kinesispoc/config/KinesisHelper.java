package com.experiment.kinesispoc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;


/**
 *  Flow goes as:
 *  1) producerN writes to stream {shard1, ..., shardN};
 *  2) consumerN reads from {applicationName, streamName}:
 *    If multiple consumers read from the same applicationName - in fact they will work as a one replica set (each reading from particular shard)
 *    Therefore to read the same data in parallel  - each consumer should read from separate applicationName
 */
@Configuration
public class KinesisHelper {


  @Value("${aws.region}")
  private String awsRegion;

  @Value("${aws.kinesisis.application.name}")
  private String kinesisAppName;

  @Value("${aws.kinesisis.stream.name}")
  private String kinesisStreamName;

  public Scheduler addConsumer() {
    DefaultCredentialsProvider credProvider = DefaultCredentialsProvider.create();
    KinesisAsyncClient kclient = KinesisAsyncClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(Region.of(awsRegion))
        .build();

    DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
        .credentialsProvider(credProvider)
        .region(Region.of(awsRegion))
        .build();

    CloudWatchAsyncClient cloudWatchAsyncClient = CloudWatchAsyncClient.builder()
        .credentialsProvider(credProvider)
        .region(Region.of(awsRegion))
        .build();

    ConfigsBuilder builder = new ConfigsBuilder(kinesisStreamName, kinesisAppName, kclient, dynamoDbAsyncClient,
        cloudWatchAsyncClient, UUID.randomUUID().toString(),
        this::recordProcessor);

    return new Scheduler(builder.checkpointConfig(), builder.coordinatorConfig(),
        builder.leaseManagementConfig(), builder.lifecycleConfig(), builder.metricsConfig(),
        builder.processorConfig(), builder.retrievalConfig());
  }


  public RecordProcessor recordProcessor() {
    return new RecordProcessor();
  }


  public void addProducer() {
    KinesisAsyncClient kclient = KinesisAsyncClient.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(Region.of(awsRegion))
        .build();


    PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
        .streamName(kinesisStreamName)
        .records(getEntries())
        .build();

    kclient.putRecords(putRecordsRequest).join();
  }

  private List<PutRecordsRequestEntry> getEntries() {

    List<PutRecordsRequestEntry> putRecordsRequests = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      PutRecordsRequestEntry putRecordsRequestEntry = PutRecordsRequestEntry.builder()
          .data(SdkBytes.fromUtf8String(String.valueOf(i)))
          .partitionKey("1")
          .build();
      putRecordsRequests.add(putRecordsRequestEntry);
    }
    return putRecordsRequests;
  }
}

