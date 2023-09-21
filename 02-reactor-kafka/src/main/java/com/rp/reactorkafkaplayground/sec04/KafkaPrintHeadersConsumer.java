package com.rp.reactorkafkaplayground.sec04;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/*
    goal: to demo events with headers
 */
@Slf4j
public class KafkaPrintHeadersConsumer {

    public static void main(String[] args) {

        var receiver = KafkaUtil.getDefaultReceiver("demo-group-another",
                "earliest",
                "demo-group-another-instance-01",
                List.of("order-events"));

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(header -> log.info("header key: {}, value: {}", header.key(), new String(header.value()))))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
