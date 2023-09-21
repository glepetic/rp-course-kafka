package com.rp.reactorkafkaplayground.sec03;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.List;

/*
    goal: to produce and consume 1 million events
 */
@Slf4j
public class KafkaConsumer {

    public static void main(String[] args) {

        var receiver = KafkaUtil.getDefaultReceiver("demo-group-another",
                "earliest",
                "demo-group-another-instance-01",
                List.of("order-events"));

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
