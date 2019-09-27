package com.experiment.kinesispoc.controller;

import com.experiment.kinesispoc.config.KinesisHelper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import software.amazon.kinesis.coordinator.Scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("stream")
@AllArgsConstructor
public class StreamController {

    private final KinesisHelper kinesisHelper;


    @GetMapping("consumer")
    public String addConsumer() {
        ExecutorService service = Executors.newFixedThreadPool(10);
        service.shutdown();
        Scheduler scheduler = kinesisHelper.addConsumer();
        Thread t = new Thread(scheduler);
        t.start();
        return "consumer added";
    }

    @GetMapping("producer")
    public String addProducer() {
        kinesisHelper.addProducer();
        return "producer added";
    }
}
