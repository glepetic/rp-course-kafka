package com.rp.reactorkafkaplayground.sec05;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/*
    goal: to demo partition re-balancing. Ensure that topic has multiple partitions

 */
@Slf4j
public class KafkaPartitionConsumer {

    public static void start(String instanceId) {

        var receiver = KafkaUtil.getDefaultReceiver("demo-group-another",
                "earliest",
                instanceId,
                List.of("order-events"));

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
