package com.experiment.kinesispoc.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;


@RequiredArgsConstructor
@Slf4j
public class RecordProcessor implements ShardRecordProcessor {

    private String shardId;

    private CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();


    @Override
    public void initialize(InitializationInput initializationInput) {
        log.info("initialized " + Thread.currentThread().getName());
        this.shardId = initializationInput.shardId();
    }


    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Initialized record processor for shard: {}", shardId);
    }


    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Reached shard end checkpointing. shardId = {}", shardId);
        checkpoint(shardEndedInput.checkpointer());
    }


    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Reached shard end checkpointing. shardId = {}", shardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }


    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        processRecordsInput.records().forEach(kinesisClientRecord -> {
            String value = null;
            try {
                value = charsetDecoder.decode(kinesisClientRecord.data()).toString();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
            System.out.println(Thread.currentThread().getName() + ": " +  value);
        });
    }


    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing for shard with id {}", this.shardId);
        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException | ShutdownException e) {
            e.printStackTrace();
        }
    }

}